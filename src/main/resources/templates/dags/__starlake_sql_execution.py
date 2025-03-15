from ai.starlake.job import StarlakeExecutionEnvironment
execution_environment = StarlakeExecutionEnvironment.SQL

statements = {{ context.statements }}

expectation_items = {{ context.expectationItems }}

audit = {{ context.audit }}

expectations = {{ context.expectations }}

acl = {{ context.acl }}
