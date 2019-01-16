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
dateSentToCompanys = FOREACH allComplaints GENERATE dateSentToCompany; 

-- group by product name
dateSentToCompanysGroup = GROUP dateSentToCompanys BY dateSentToCompany;

-- count how many times each product name appers
dateSentToCompanysCount = FOREACH dateSentToCompanysGroup GENERATE group AS dateSentToCompanyName:chararray, COUNT(dateSentToCompanys) AS count:long;

-- sort by number of appearance
dateSentToCompanysDesc = ORDER dateSentToCompanysCount BY count DESC;

first100 = LIMIT dateSentToCompanysDesc 100;

-- print on terminal
DUMP first100;
