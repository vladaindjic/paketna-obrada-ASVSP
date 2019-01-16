/*
Koliko je najvise, odnosno najmanje vremena trebalo da se prosledi zalba u zavisnosti od sredstav kojim je zalba podneta. Koliko je prosek za slanje zalbe kompaniji u zavisnosti od sredstva putem koga je zalba podneta.
*/


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



-- calculate difference between date, convert daters, get company, product, submitedVia
daysBetweenReceivedAndSent = FOREACH allComplaints GENERATE DaysBetween(ToDate(dateSentToCompany, 'MM/dd/yyyy'), ToDate(dateReceived, 'MM/dd/yyyy')) as (daysBetween:long),
						            dateSentToCompany,
							    dateReceived,
							    company,
						            product,
	                                                    submittedVia; 


-- some days are negative, so we should remove them
daysBetweenReceivedAndSent = FILTER daysBetweenReceivedAndSent BY daysBetween >=0;

----------------------------------------------------------------------------------------------------------------------
-- max per group (with nested foreach)
-- see for explanation https://stackoverflow.com/questions/29294411/apache-pig-trying-to-get-max-count-in-each-group
groupBySubmittedVia = GROUP daysBetweenReceivedAndSent BY submittedVia;
maxDaysPerSubmittedVia = foreach groupBySubmittedVia {
    days = ORDER daysBetweenReceivedAndSent BY daysBetween DESC;
    topDays = limit days 1;
    generate flatten(topDays);
}
orderDescMaxDaysPerSubmittedVia = ORDER maxDaysPerSubmittedVia BY daysBetween DESC;
--dump orderDescMaxDaysPerSubmittedVia;
/*
Output:
(1962,08/28/2017,04/14/2012,BANK OF THE WEST,Credit card,Web)
(1753,08/28/2017,11/08/2012,EQUIFAX, INC.,Credit reporting,Fax)
(1601,08/28/2017,04/10/2013,EQUIFAX, INC.,Credit reporting,Referral)
(1188,08/28/2017,05/28/2014,SYNCHRONY FINANCIAL,Credit card,Postal mail)
(1019,03/16/2018,06/01/2015,Jim Bottin Enterprises, Inc,Debt collection,Phone)
(189,10/17/2012,04/11/2012,STATE EMPLOYEES CREDIT UNION,Bank account or service,Email)


*/

--STORE orderDescMaxDaysPerSubmittedVia INTO '/home/complaints/maxDaysPerSubmittedVia' using CSVExcelStorage(','); 


---------------------------------------------------------------------------------------------------------------------


-- min per group (with nested foreach)
groupBySubmittedVia = GROUP daysBetweenReceivedAndSent BY submittedVia;
minDaysPerSubmittedVia = foreach groupBySubmittedVia {
    days = ORDER daysBetweenReceivedAndSent BY daysBetween ASC;
    bottomDays = limit days 1;
    generate flatten(bottomDays);
}
orderAscMinDaysPerSubmittedVia = ORDER minDaysPerSubmittedVia BY daysBetween ASC;
--dump orderAscMinDaysPerSubmittedVia;
/*
Output:

(0,08/22/2018,08/22/2018,TRANSUNION INTERMEDIATE HOLDINGS, INC.,Credit reporting, credit repair services, or other personal consumer reports,Postal mail)
(0,08/27/2018,08/27/2018,Experian Information Solutions Inc.,Credit reporting, credit repair services, or other personal consumer reports,Referral)
(0,10/12/2017,10/12/2017,WELLS FARGO & COMPANY,Money transfer, virtual currency, or money service,Phone)
(0,06/28/2013,06/28/2013,TRANSUNION INTERMEDIATE HOLDINGS, INC.,Credit reporting,Email)
(0,04/09/2018,04/09/2018,EQUIFAX, INC.,Credit reporting, credit repair services, or other personal consumer reports,Web)
(0,01/25/2018,01/25/2018,TRANSUNION INTERMEDIATE HOLDINGS, INC.,Credit reporting, credit repair services, or other personal consumer reports,Fax)

*/

--STORE orderAscMinDaysPerSubmittedVia INTO '/home/complaints/minDaysPerSubmittedVia' using CSVExcelStorage(','); 




-- average per group
groupBySubmittedVia = GROUP daysBetweenReceivedAndSent BY submittedVia;
avgDaysPerSubmittedVia = FOREACH groupBySubmittedVia GENERATE group, AVG(daysBetweenReceivedAndSent.daysBetween) as (averageDays:double);
orderAscAvgDaysPerSubmittedVia = ORDER avgDaysPerSubmittedVia BY averageDays ASC;
--dump orderAscAvgDaysPerSubmittedVia;

/*
Output:

(Web,2.4683356551084192)
(Phone,4.207157781031028)
(Fax,4.727353168897245)
(Referral,5.804546471616302)
(Postal mail,7.264034938284989)
(Email,7.729222520107238)

*/

STORE orderAscAvgDaysPerSubmittedVia INTO '/home/complaints/avgDaysPerSubmittedVia' using CSVExcelStorage(','); 


































/*
-- find max time to response
ordDesc = ORDER daysBetweenReceivedAndSent BY daysBetween DESC;
maxTimeToResponse = LIMIT ordDesc 1;
dump maxTimeToResponse;
-- (1962, 08/28/2017, 04/14/2012, BANK OF THE WEST, Credit card, Web)
-- to gen output dir
STORE maxTimeToResponse INTO '/home/complaints/maxTimeToResponse' USING CSVExcelStorage(','); 


-- remove negative numbers (some of them are)
nonNegative = FILTER daysBetweenReceivedAndSent BY daysBetween >= 0;
-- find min
orderAsc = ORDER nonNegative BY daysBetween ASC;
minTimeToResponse = LIMIT orderAsc 1;
dump minTimeToResponse;
-- (0, 11/09/2015, 11/09/2015, EXETER FINANCE CORP., Debt collection, Web)
-- to gen output dir
STORE minTimeToResponse INTO '/home/complaints/minTimeToResponse' USING CSVExcelStorage(','); 



-- number of response that are sent immediately, depending on submittedVia
sendImmediately = FILTER daysBetweenReceivedAndSent BY daysBetween == 0;
*/

















































/*
-- Jedan od nacina trazenja maksimuma, ali nije bas sjajan
dbrsGroupAll = GROUP daysBetweenReceivedAndSent all;
maxDbrs = FOREACH dbrsGroupAll GENERATE daysBetweenReceivedAndSent.dateSentToCompany, daysBetweenReceivedAndSent.dateReceived, MAX(daysBetweenReceivedAndSent.daysBetween);




-- to generate dates
receivedAndSentDates = FOREACH allComplaints GENERATE ToDate(dateReceived, 'MM/dd/yyyy') AS (dateReceived:DateTime), ToDate(dateSentToCompany, 'MM/dd/yyyy') AS (dateSentToCompany:DateTime);




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


*/
