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

-- projection on property product
subProducts = FOREACH allComplaints GENERATE subProduct; 

-- group by product name
subProductsGroup = GROUP subProducts BY subProduct;

-- count how many times each product name appers
subProductsCount = FOREACH subProductsGroup GENERATE group AS subProductName:chararray, COUNT(subProducts) AS count:long;

-- sort by number of appearance
sortSubProductsDesc = ORDER subProductsCount BY count DESC;

-- print on terminal
DUMP sortSubProductsDesc;
