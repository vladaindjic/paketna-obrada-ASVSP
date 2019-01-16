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
consumerConsentProvideds = FOREACH allComplaints GENERATE consumerConsentProvided; 

-- group by product name
consumerConsentProvidedsGroup = GROUP consumerConsentProvideds BY consumerConsentProvided;

-- count how many times each product name appers
consumerConsentProvidedsCount = FOREACH consumerConsentProvidedsGroup GENERATE group AS consumerConsentProvidedName:chararray, COUNT(consumerConsentProvideds) AS count:long;

-- sort by number of appearance
consumerConsentProvidedsDesc = ORDER consumerConsentProvidedsCount BY count DESC;

first100 = LIMIT consumerConsentProvidedsDesc 100;

-- print on terminal
DUMP first100;