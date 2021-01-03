package sprinter

import (
	"database/sql"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/ssrdive/mysequel"
	"github.com/ssrdive/sprinter/queries"
)

// ContractScheduleDueRental holds daily installments
// that needs to be issued income journal entries
type ContractScheduleDueRental struct {
	ID          int
	ContractID  int
	Capital     float64
	Interest    float64
	MonthlyDate string
}

// ContractFinancial holds financial summary
// related to contracts
type ContractFinancial struct {
	Active             int
	RecoveryStatus     int
	Doubtful           int
	Payment            float64
	CapitalArrears     float64
	InterestArrears    float64
	CapitalProvisioned float64
	ScheduleEndDate    string
}

// JournalEntry holds a journal entry
// to be inserted to the financials
type JournalEntry struct {
	Account string
	Debit   string
	Credit  string
}

// UpdatedContract holds details of updates done
type UpdatedContract struct {
	ContractID            int
	RecoveryStatus        int
	UpdatedRecoveryStatus int
}

const (
	// UnearnedInterestAccount holds account database id
	UnearnedInterestAccount = 188
	// InterestIncomeAccount holds account database id
	InterestIncomeAccount = 190
	// ReceivableAccount holds account database id
	ReceivableAccount = 185
	// ReceivableArrearsAccount holds account database id
	ReceivableArrearsAccount = 192
	// SuspenseInterestAccount holds account database id
	SuspenseInterestAccount = 194
	// BadDebtProvisionAccount holds account database id
	BadDebtProvisionAccount = 195
	// ProvisionForBadDebtAccount holds account database id
	ProvisionForBadDebtAccount = 196

	// RecoveryStatusActive holds status database id
	RecoveryStatusActive = 1
	// RecoveryStatusArrears holds status database id
	RecoveryStatusArrears = 2
	// RecoveryStatusNPL holds status database id
	RecoveryStatusNPL = 3
	// RecoveryStatusBDP holds status database id
	RecoveryStatusBDP = 4

	// DoubtfulStatusYes holds boolean yes
	DoubtfulStatusYes = 1
	// DoubtfulStatusNo holds boolean no
	DoubtfulStatusNo = 0
)

// Run runs the day end program for all system or for a single contract
func Run(date, contract string, manual bool, tx *sql.Tx) ([]UpdatedContract, time.Duration, error) {
	start := time.Now()
	var err error
	var dueRentals []ContractScheduleDueRental
	if manual {
		err = mysequel.QueryToStructs(&dueRentals, tx, queries.DueRentalsByContract, date, contract)
	} else {
		err = mysequel.QueryToStructs(&dueRentals, tx, queries.DueRentals, date)
	}
	if err != nil {
		return nil, 0, err
	}

	updatedContracts := []UpdatedContract{}

	for _, rental := range dueRentals {
		updatedContract := UpdatedContract{
			ContractID: rental.ContractID,
		}
		// Obtain active / period over, arrears status, last installment date
		var cF ContractFinancial
		err = tx.QueryRow(queries.ContractFinancial, rental.ContractID).Scan(&cF.Active, &cF.RecoveryStatus, &cF.Doubtful, &cF.Payment, &cF.CapitalArrears, &cF.InterestArrears, &cF.CapitalProvisioned, &cF.ScheduleEndDate)
		if err != nil {
			return nil, 0, err
		}
		updatedContract.RecoveryStatus = cF.RecoveryStatus
		updatedContract.UpdatedRecoveryStatus = cF.RecoveryStatus

		// Change contract active to 0 if the period is over
		if cF.ScheduleEndDate == rental.MonthlyDate {
			_, err = mysequel.Update(mysequel.UpdateTable{
				Table: mysequel.Table{TableName: "contract_financial",
					Columns: []string{"active"},
					Vals:    []interface{}{0},
					Tx:      tx},
				WColumns: []string{"contract_id"},
				WVals:    []string{strconv.FormatInt(int64(rental.ContractID), 10)},
			})
			if err != nil {
				return nil, 0, err
			}
		}

		// Creating the transaction for day-end program journal entry.
		tid, err := mysequel.Insert(mysequel.Table{
			TableName: "transaction",
			Columns:   []string{"user_id", "datetime", "posting_date", "contract_id", "remark"},
			Vals:      []interface{}{1, time.Now().Format("2006-01-02 15:04:05"), time.Now().Format("2006-01-02"), rental.ContractID, fmt.Sprintf("DAY END %d [%d]", rental.ID, rental.ContractID)},
			Tx:        tx,
		})
		if err != nil {
			return nil, 0, err
		}

		/*
			Active
				nAge <= 0
					* Rental interest to income
					* Rental amount to arrears
				nAge > 0
					* Rental interest to income
					* Rental amount to arrears
					* Update contract status to arrears
			Arrears
				Doubtful
					Yes
						nAge >= 6
							* 50% capital provision
							* Rental interest to suspense
							* Rental amount to arrears
							* Update contract status to NPL and doubtful to yes
						nAge < 6
							* Rental interest to suspense
							* Rental amount to arrears
					No
						nAge >= 6
							* 50% capital provision
							* Arrears interest to suspense
							* Rental interest to suspense
							* Rental amount to arrears
							* Update contract status to NPL and doubtful to yes
						nAge < 6
							* Rental interest to income
							* Rental amount to arrearss
			NPL
				nAge >= 12
					* Balance capital provision to 100%
					* Rental interest to suspense
					* Rental amount to arrears
					* Update contract status to BDP
				nAge < 12
					* Rental interest to suspense
					* Rental amount to arrears
			BDP
				* Rental interest to suspense
				* Rental amount to arrears
		*/

		arrears := cF.CapitalArrears + cF.InterestArrears
		nAge := (arrears + rental.Capital + rental.Interest) / cF.Payment
		dayEndJEs := []JournalEntry{}
		if cF.RecoveryStatus == RecoveryStatusActive && nAge <= 0 {
			dayEndJEs = append(dayEndJEs, interestJEs("Income", rental)...)
		} else if cF.RecoveryStatus == RecoveryStatusActive && nAge > 0 {
			dayEndJEs = append(dayEndJEs, interestJEs("Income", rental)...)
			_, err = tx.Exec(queries.UpdateRecoveryStatus, RecoveryStatusArrears, rental.ContractID)
			if err != nil {
				return nil, 0, err
			}
			updatedContract.UpdatedRecoveryStatus = RecoveryStatusArrears
		} else if cF.RecoveryStatus == RecoveryStatusArrears && cF.Doubtful == 1 && nAge >= 6 {
			var capitalProvision float64
			err = tx.QueryRow(queries.NplCapitalProvision, rental.ContractID).Scan(&capitalProvision)
			if err != nil {
				return nil, 0, err
			}
			dayEndJEs = append(dayEndJEs, capitalProvisionJEs(capitalProvision)...)
			dayEndJEs = append(dayEndJEs, interestJEs("Suspense", rental)...)
			_, err = tx.Exec(queries.UpdateRecoveryStatus, RecoveryStatusNPL, rental.ContractID)
			if err != nil {
				return nil, 0, err
			}
			_, err = tx.Exec(queries.UpdateCapitalProvisioned, capitalProvision, rental.ContractID)
			if err != nil {
				return nil, 0, err
			}
			updatedContract.UpdatedRecoveryStatus = RecoveryStatusNPL
		} else if cF.RecoveryStatus == RecoveryStatusArrears && cF.Doubtful == 1 && nAge < 6 {
			dayEndJEs = append(dayEndJEs, interestJEs("Suspense", rental)...)
		} else if cF.RecoveryStatus == RecoveryStatusArrears && cF.Doubtful == 0 && nAge >= 6 {
			var capitalProvision float64
			err = tx.QueryRow(queries.NplCapitalProvision, rental.ContractID).Scan(&capitalProvision)
			if err != nil {
				return nil, 0, err
			}
			dayEndJEs = append(dayEndJEs, capitalProvisionJEs(capitalProvision)...)
			dayEndJEs = append(dayEndJEs, incomeToSuspenseJEs(cF.InterestArrears)...)
			dayEndJEs = append(dayEndJEs, interestJEs("Suspense", rental)...)
			_, err = tx.Exec(queries.UpdateRecoveryDoubtfulStatus, RecoveryStatusNPL, 1, rental.ContractID)
			if err != nil {
				return nil, 0, err
			}
			_, err = tx.Exec(queries.UpdateCapitalProvisioned, capitalProvision, rental.ContractID)
			if err != nil {
				return nil, 0, err
			}
			updatedContract.UpdatedRecoveryStatus = RecoveryStatusNPL
		} else if cF.RecoveryStatus == RecoveryStatusArrears && cF.Doubtful == 0 && nAge < 6 {
			dayEndJEs = append(dayEndJEs, interestJEs("Income", rental)...)
		} else if cF.RecoveryStatus == RecoveryStatusNPL && nAge >= 12 {
			var capitalReceivable float64
			err = tx.QueryRow(queries.CapitalReceivable, rental.ContractID).Scan(&capitalReceivable)
			if err != nil {
				return nil, 0, err
			}
			capitalProvision := math.Round((capitalReceivable-cF.CapitalProvisioned)*100) / 100
			dayEndJEs = append(dayEndJEs, capitalProvisionJEs(capitalProvision)...)
			dayEndJEs = append(dayEndJEs, interestJEs("Suspense", rental)...)
			_, err = tx.Exec(queries.UpdateCapitalProvisioned, capitalProvision, rental.ContractID)
			if err != nil {
				return nil, 0, err
			}
			_, err = tx.Exec(queries.UpdateRecoveryStatus, RecoveryStatusBDP, rental.ContractID)
			if err != nil {
				return nil, 0, err
			}
			updatedContract.UpdatedRecoveryStatus = RecoveryStatusBDP
		} else if cF.RecoveryStatus == RecoveryStatusNPL && nAge < 12 {
			dayEndJEs = append(dayEndJEs, interestJEs("Suspense", rental)...)
		} else if cF.RecoveryStatus == RecoveryStatusBDP {
			dayEndJEs = append(dayEndJEs, interestJEs("Suspense", rental)...)
		}

		err = issueJournalEntries(tx, tid, dayEndJEs)
		if err != nil {
			return nil, 0, err
		}

		_, err = tx.Exec(queries.UpdateArrears, rental.Capital, rental.Interest, rental.ContractID)
		if err != nil {
			return nil, 0, err
		}

		_, err = mysequel.Update(mysequel.UpdateTable{
			Table: mysequel.Table{TableName: "contract_schedule",
				Columns: []string{"daily_entry_issued"},
				Vals:    []interface{}{1},
				Tx:      tx},
			WColumns: []string{"id"},
			WVals:    []string{strconv.FormatInt(int64(rental.ID), 10)},
		})
		if err != nil {
			return nil, 0, err
		}
		updatedContracts = append(updatedContracts, updatedContract)
	}
	return updatedContracts, time.Since(start), err
}

func incomeToSuspenseJEs(interest float64) []JournalEntry {
	return []JournalEntry{{fmt.Sprintf("%d", InterestIncomeAccount), fmt.Sprintf("%f", interest), ""},
		{fmt.Sprintf("%d", SuspenseInterestAccount), "", fmt.Sprintf("%f", interest)}}
}

func suspenseJEs(interest float64) []JournalEntry {
	return []JournalEntry{{fmt.Sprintf("%d", UnearnedInterestAccount), fmt.Sprintf("%f", interest), ""},
		{fmt.Sprintf("%d", SuspenseInterestAccount), "", fmt.Sprintf("%f", interest)}}
}

func interestJEs(intrType string, rental ContractScheduleDueRental) []JournalEntry {
	journalEntries := []JournalEntry{
		{fmt.Sprintf("%d", ReceivableArrearsAccount), fmt.Sprintf("%f", rental.Capital+rental.Interest), ""},
		{fmt.Sprintf("%d", ReceivableAccount), "", fmt.Sprintf("%f", rental.Capital+rental.Interest)}}
	if intrType == "Income" {
		journalEntries = append(journalEntries, JournalEntry{fmt.Sprintf("%d", UnearnedInterestAccount), fmt.Sprintf("%f", rental.Interest), ""},
			JournalEntry{fmt.Sprintf("%d", InterestIncomeAccount), "", fmt.Sprintf("%f", rental.Interest)})
	} else if intrType == "Suspense" {
		journalEntries = append(journalEntries, suspenseJEs(rental.Interest)...)
	}
	return journalEntries
}

func capitalProvisionJEs(capitalProvision float64) []JournalEntry {
	return []JournalEntry{
		// Bad Debt Provision
		{fmt.Sprintf("%d", BadDebtProvisionAccount), fmt.Sprintf("%f", capitalProvision), ""},
		{fmt.Sprintf("%d", ProvisionForBadDebtAccount), "", fmt.Sprintf("%f", capitalProvision)},
	}
}

func issueJournalEntries(tx *sql.Tx, tid int64, journalEntries []JournalEntry) error {
	for _, entry := range journalEntries {
		if len(entry.Debit) != 0 {
			_, err := mysequel.Insert(mysequel.Table{
				TableName: "account_transaction",
				Columns:   []string{"transaction_id", "account_id", "type", "amount"},
				Vals:      []interface{}{tid, entry.Account, "DR", entry.Debit},
				Tx:        tx,
			})
			if err != nil {
				return err
			}
		}
		if len(entry.Credit) != 0 {
			_, err := mysequel.Insert(mysequel.Table{
				TableName: "account_transaction",
				Columns:   []string{"transaction_id", "account_id", "type", "amount"},
				Vals:      []interface{}{tid, entry.Account, "CR", entry.Credit},
				Tx:        tx,
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func openDB(dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		return nil, err
	}
	return db, err
}
