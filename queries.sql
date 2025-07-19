// Create storage integration object
CREATE OR REPLACE STORAGE INTEGRATION S3_lag
TYPE = external_stage
STORAGE_PROVIDER = s3
ENABLED = true
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<Account ID>:role/'
STORAGE_ALLOWED_LOCATIONS = ('s3://<bucketname>');

// Get external_id and update it in S3
desc STORAGE INTEGRATION S3_lag;   

// ARN -- Amazon Resource Names
// S3 -- Simple Storage Service
// IAM -- Identity and Access Management


// Create file format object
CREATE OR REPLACE file format mydb.file_formats.fileformat_csv
    type = csv
    field_delimiter = ','
    skip_header = 1
    empty_field_as_null = TRUE;


// Create stage object with integration object & file format object
 CREATE OR REPLACE STAGE mydb.external_stages.csv_s3_aws
    URL = ('s3://<bucketname>')
    STORAGE_INTEGRATION = s3_lag
    FILE_FORMAT = mydb.file_formats.fileformat_csv ;   


//Listing files under your s3 buckets
 list @mydb.external_stages.csv_s3_aws;


// Create a table first
CREATE OR REPLACE TABLE mydb.public.lag_table (
    ID INT PRIMARY KEY,
    Name VARCHAR(255),
    Age INT,
    Country VARCHAR(100),
    Email VARCHAR(255),
    Phone VARCHAR(50),
    Address TEXT,
    Company VARCHAR(255),
    DateJoined DATE,
    Salary DECIMAL(10,2)
);


// Use Copy command to load the files
COPY INTO mydb.public.lag_table
    FROM @mydb.external_stages.csv_s3_aws
    FILE_FORMAT = (format_name=fileformat_csv);
	
 //Validate the data      
select * from mydb.public.lag_table; 

Scenario : 1

COPY INTO mydb.public.lag_table
    FROM @mydb.external_stages.csv_s3_aws
    FILE_FORMAT = (format_name=fileformat_csv)
	on_error = 'skip'; -- Skip the whole file 	 	
	
COPY INTO mydb.public.lag_table
    FROM @mydb.external_stages.csv_s3_aws
    FILE_FORMAT = (format_name=fileformat_csv)
	on_error = 'CONTINUE'; --Skip only the Bad records and load the rest of the records

COPY INTO mydb.public.lag_table
    FROM @mydb.external_stages.csv_s3_aws
    FILE_FORMAT = (format_name=fileformat_csv)
	on_error = 'abort';	-- If any error occurs, stop the process and do not load any data
	
Scenario : 2

COPY INTO mydb.public.lag_table
    FROM @mydb.external_stages.csv_s3_aws
    FILE_FORMAT = (format_name=fileformat_csv)
	FORCE = true; -- Reloads all files even if they were previously loaded

Scenario : 3

COPY INTO mydb.public.lag_table
    FROM @mydb.external_stages.csv_s3_aws
    FILE_FORMAT = (format_name=fileformat_csv)
	PURGE = true; -- Deletes the source files from the stage after a successful load
