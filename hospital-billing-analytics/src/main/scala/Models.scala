case class Visit(
  visit_id: String,
  patient_id: String,
  visit_date: String,
  department: String,
  doctor_id: String
)

case class Procedure(
  procedure_id: String,
  visit_id: String,
  procedure_name: String,
  procedure_cost: Double
)

case class Insurance(
  patient_id: String,
  insurance_provider: String,
  coverage_percent: Double
)

case class Payment(
  payment_id: String,
  visit_id: String,
  payment_date: String,
  payment_amount: Double,
  payment_method: String
)

case class BillingRecord(
  visit_id: String,
  patient_id: String,
  department: String,
  total_bill: Double,
  insurance_covered: Double,
  amount_paid: Double,
  outstanding_balance: Double
)
