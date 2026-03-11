import great_expectations as gx
import duckdb
import pandas as pd
from datetime import datetime

# Load data from DuckDB
con = duckdb.connect("data/payments.duckdb")
df = con.execute("SELECT * FROM stg_transactions").fetchdf()
con.close()

# Create GX context
context = gx.get_context(mode="ephemeral")

# Create data source
data_source = context.data_sources.add_pandas("payments")
data_asset = data_source.add_dataframe_asset("transactions")
batch_def = data_asset.add_batch_definition_whole_dataframe("full_batch")
batch = batch_def.get_batch(batch_parameters={"dataframe": df})

# Create expectation suite
suite = context.suites.add(gx.ExpectationSuite(name="payments_suite"))

# Define expectations
expectations = [
    gx.expectations.ExpectColumnValuesToNotBeNull(column="transaction_id"),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="amount"),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="status"),
    gx.expectations.ExpectColumnValuesToNotBeNull(column="country"),
    gx.expectations.ExpectColumnValuesToBeUnique(column="transaction_id"),
    gx.expectations.ExpectColumnValuesToBeBetween(column="amount", min_value=0, max_value=100000),
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="status",
        value_set=["approved", "declined", "pending", "reversed"]
    ),
    gx.expectations.ExpectColumnValuesToBeInSet(
        column="country",
        value_set=["US", "GB", "ZA", "NG", "KE", "AE", "SG", "BR", "MX", "IN"]
    ),
    gx.expectations.ExpectColumnMeanToBeBetween(column="amount", min_value=50, max_value=300),
    gx.expectations.ExpectTableRowCountToBeBetween(min_value=1000, max_value=1000000),
]

for exp in expectations:
    suite.add_expectation(exp)

# Run validation
validation_def = context.validation_definitions.add(
    gx.ValidationDefinition(
        name="payments_validation",
        data=batch_def,
        suite=suite
    )
)

results = validation_def.run(batch_parameters={"dataframe": df})

# Print results
print("\n" + "="*60)
print("GREAT EXPECTATIONS VALIDATION RESULTS")
print("="*60)
print(f"Success: {results.success}")
print(f"Total expectations: {len(results.results)}")
passed = sum(1 for r in results.results if r.success)
failed = sum(1 for r in results.results if not r.success)
print(f"Passed: {passed}")
print(f"Failed: {failed}")

print("\nDetailed Results:")
for r in results.results:
    status = " PASS" if r.success else " FAIL"
    print(f"  {status} — {r.expectation_config.type}")

# Save results to log file
with open("logs/validation_log.txt", "a") as f:
    f.write(f"\n{datetime.now()} | Success: {results.success} | Passed: {passed} | Failed: {failed}\n")

print("\nResults logged to logs/validation_log.txt")

if not results.success:
    exit(1)