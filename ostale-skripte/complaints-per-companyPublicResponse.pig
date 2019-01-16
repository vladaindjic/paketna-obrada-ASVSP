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

-- projection on property tag
companyPublicResponses = FOREACH allComplaints GENERATE companyPublicResponse; 

-- group by product name
companyPublicResponsesGroup = GROUP companyPublicResponses BY companyPublicResponse;

-- count how many times each product name appers
companyPublicResponsesCount = FOREACH companyPublicResponsesGroup GENERATE group AS companyPublicResponseName:chararray, COUNT(companyPublicResponses) AS count:long;

-- sort by number of appearance
companyPublicResponsesDesc = ORDER companyPublicResponsesCount BY count DESC;

first100 = LIMIT companyPublicResponsesDesc 100;

-- print on terminal
DUMP first100;
