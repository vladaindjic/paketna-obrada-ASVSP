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
tagses = FOREACH allComplaints GENERATE tags; 

-- group by product name
tagsesGroup = GROUP tagses BY tags;

-- count how many times each product name appers
tagsesCount = FOREACH tagsesGroup GENERATE group AS tagsName:chararray, COUNT(tagses) AS count:long;

-- sort by number of appearance
sortTagsesDesc = ORDER tagsesCount BY count DESC;

-- print on terminal
DUMP sortTagsesDesc;
