package queries

const (
	DueRentals = `
		SELECT CS.id, CS.contract_id, CS.capital, CS.interest, CS.monthly_date
		FROM contract_schedule CS
		WHERE CS.daily_entry_issued = 0 AND CS.monthly_date <= ? AND CS.contract_installment_type_id = 1
		ORDER BY CS.contract_id ASC, CS.monthly_date ASC
	`

	DueRentalsByContract = `
		SELECT CS.id, CS.contract_id, CS.capital, CS.interest, CS.monthly_date
		FROM contract_schedule CS
		WHERE CS.daily_entry_issued = 0 AND CS.monthly_date <= ? AND CS.contract_installment_type_id = 1 AND CS.contract_id = ?
		ORDER BY CS.contract_id ASC, CS.monthly_date ASC
	`

	ContractFinancial = `
		SELECT CF.active, CF.recovery_status_id, CF.doubtful, CF.payment, CF.capital_arrears, CF.interest_arrears, CF.capital_provisioned, CF.financial_schedule_end_date
		FROM contract_financial CF
		WHERE CF.contract_id = ?
	`

	UpdateRecoveryStatus = `
		UPDATE contract_financial SET recovery_status_id = ? WHERE contract_id = ?
	`

	UpdateDoubtfulStatus = `
		UPDATE contract_financial SET doubtful = ? WHERE contract_id = ?
	`

	UpdateRecoveryDoubtfulStatus = `
		UPDATE contract_financial SET recovery_status_id = ?, doubtful = ? WHERE contract_id = ?
	`

	NplCapitalProvision = `
		SELECT ROUND(SUM(CS.capital-CS.capital_paid)/2, 2) AS capital_provision
		FROM contract_schedule CS
		WHERE CS.contract_id = ? AND CS.contract_installment_type_id = 1
		GROUP BY CS.contract_id
	`

	UpdateCapitalProvisioned = `
		UPDATE contract_financial SET capital_provisioned = capital_provisioned + ? WHERE contract_id = ?
	`

	UpdateCapitalProvisionedBDP = `
		UPDATE contract_financial SET capital_provisioned = capital_provisioned + ?, capital_provisioned_bdp = capital_provisioned_bdp + ? WHERE contract_id = ?
	`

	CapitalReceivable = `
		SELECT SUM(CS.capital-CS.capital_paid) AS capital_receivable
		FROM contract_schedule CS
		WHERE CS.contract_id = ? AND CS.contract_installment_type_id = 1
		GROUP BY CS.contract_id
	`

	CapitalProvisionedAmount = `
		SELECT capital_provisioned
		FROM contract_financial
		WHERE contract_id = ?
	`

	UpdateArrears = `
		UPDATE contract_financial SET capital_arrears = capital_arrears + ?, interest_arrears = interest_arrears + ? WHERE contract_id = ?
	`
)
