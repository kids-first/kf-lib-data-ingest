# Metrics

## Tests
1. **Unique Count Tests** 

    N - total expected unique entity instances for the study
    
    M - actual number of unique entity instances in the input data to accounting

    For any of these entities (participant, biospecimen, source sequencing file) that
    fail the check (N - M == 0), output this:

    Example:

    ```
    N = 150 expected participants
    M = 110 actual unique participants found

    Participant Unique IDs and Locations:
    {
      'p1':  ['s3://file1.tsv', 's3://file2.tsv'],
      'p2':  ['s3://file1.tsv', 's3://file2.tsv'],
      'p3':  ['s3://file1.tsv']
    }

    Summary (maybe):
    {
      'file1.tsv':  100 participants,
      'file2.tsv':  101 participants
    }
    ```
    
2. **Relationship Tests**

    For any parent:child entity (like participant and biospecimen) association, compute the following:

    Y - Number and list of unique parent entity instances with 0 children
    
    Z - Number and list of unique children entity instances with no parent

    Example: 

    ```
    Y = 1 participant with 0 biospecimens found
    Z = 1 biospecimen with 0 participants found
    
    # of participants with 0 biospecimens and their locations
    {
      'p1': ['file1.tsv', 'file2.tsv'] 
    }
    
    # of biospecimens with 0 participant and their locations
    {
      'b1': ['file1.tsv', 'file2.tsv'] 
    }
    ```

## Input Required 
- Expected counts of entities:
  - participant
  - biospecimen
  - source sequencing files
  - nuclear families (maybe)

## Configuration
- The default set of tests are the ones defined above
- Every study will run the default set of tests in accounting
- Want to be able to easily define and plug in custom tests per study
- Want to be able to turn on/off tests on a per study basis
- Want to pass in expected values for tests on a per study basis

## User Interface

```
$ kidsfirst validate <path to study dir>
```
Under the hood this will run all stages of the ingest pipeline except load.

## Execution
Accounting will run after extract stage and after transform
