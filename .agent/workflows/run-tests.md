---
description: Run tests with minimal output, redirecting verbose logs to files
---

# Run Tests Workflow

// turbo-all

This workflow runs maven tests with output redirected to a file and extracts only the summary.

## Steps

1. Run all tests in nishi-utils-core:

```bash
cd /home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core && mvn test 2>&1 | tee /tmp/nishi-test-output.txt
```

2. Extract only the test summary (pass/fail counts per class):

```bash
grep -E 'Tests run:' /tmp/nishi-test-output.txt | sed 's/\x1b\[[0-9;]*m//g'
```

3. If there are failures, extract error details:

```bash
grep -E '^\[ERROR\]' /tmp/nishi-test-output.txt | sed 's/\x1b\[[0-9;]*m//g'
```

4. Check total output size to confirm verbosity reduction:

```bash
wc -l /tmp/nishi-test-output.txt
```

5. For detailed logs of a specific failed test, check surefire reports:

```bash
cat /home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core/target/surefire-reports/<TestClassName>.txt
```

## Running a Single Test

```bash
cd /home/lucas/Projects/nishisan/nishi-utils/nishi-utils-core && mvn test -Dtest=<TestClassName> 2>&1 | tee /tmp/nishi-test-single.txt
```

## Running Tests by Profile

- **Resilience (in-process NGrid):** `mvn test -Presilience`
- **Docker Resilience (Testcontainers):** `mvn verify -Pdocker-resilience`
- **Soak (long-running):** `mvn test -Psoak`
