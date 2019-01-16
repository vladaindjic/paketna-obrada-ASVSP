-- set CSV parser, beacuse of commas that are between quotes
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage();

-- loading all complaints
allComplaints = LOAD '/home/complaints/complaints-valid.csv'
   USING CSVExcelStorage(',')
   as ( dateReceived: chararray,
        product: chararray,
        subProduct: chararray,
        issue: chararray,
        subIssue: chararray,
        consumerComplaintNarrative: chararray,
        companyPublicResponse: chararray,
        company: chararray,
        state: chararray,
        zipCode: chararray,
        tags: chararray,
        consumerConsentProvided: chararray,
        submittedVia: chararray,
        dateSentToCompany: chararray,
        companyResponseToConsumer: chararray,
        timelyResponse: chararray,
        consumerDisputed: chararray,
        complaintId: chararray);

-- projection on propery company
companies = FOREACH allComplaints GENERATE company; 

-- group by company name
companiesGroup = GROUP companies BY company;

-- count how many times each company name appers
companiesCount = FOREACH companiesGroup GENERATE group AS companyName:chararray, COUNT(companies) AS count:long;

-- sort by number of appearance
sortCompaniesDesc = ORDER companiesCount BY count DESC;

-- print on terminal
DUMP sortCompaniesDesc;

-- get first 100
-- first100 = LIMIT sortCompaniesDesc 100;

-- store
-- STORE first100 INTO '/home/complaints/generated/complaints-per-company' USING CSVExcelStorage(',');
