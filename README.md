# Projektni zadatak za potrebe predmeta ASVSP - paketna obrada podataka
Tema projektnog zadataka iz predmeta Arhitektura sistema velikih skupova podataka je analiza žalbi korisnika 
na usluge finansijskih kompanija na prostoru Sjedinjenih Američkih Država. 
Pročišćeni skup podataka (u nastavku skup podataka) dostupan je na 
[linku](https://drive.google.com/open?id=1giNPVFy6ACl3bvfj6hu1vR6GWGWXkQaI), dok je uvid u originalni skup podataka, 
koji se redovno ažurira, dostupan na [linku](https://www.consumerfinance.gov/data-research/consumer-complaints/search/?from=0&searchField=all&searchText=&size=25&sort=created_date_desc).

## Motivacija
Na svakodnevnom nivou pristiže veliki broj žalbi na usluge američkih finansijskih kompanija, pretežno banki.
Bankama je u interesu da zadrže što veći broj klijenata, te je neophodno da dosta pažnje posvete žalbama na usluge koje 
pružaju. Manuelna analiza velikog broja žalbi je mukotrpna, te bi bilo zgodno ukoliko bi se taj proces automatizovao.
Ovo se može uraditi upotrebom Apache Hadoop-a i njegovog MapReduce-a. 

Pisanje MapReduce zadataka zna biti naporno, jer zahteva poznavanje nekog od programskih jezika 
koje Hadoop podržava, ali i poznavanje načina rada samog Hadoop-a. Zbog toga se u ovom projektu se 
analiza skupa podataka vrši upotrebom Apache Pig alata. U pitanju je alat koji koristi
posebno definisan Pig Latin jezik za iskazivanje analitičkih upita. Skripte pisane ovim jezikom se prevode na MapReduce
zadatke, koji se izvršavaju u okviru Hadoop-a. Razlog zbog koga sam se odlučio da koristim Apache Pig, a ne recimo Hive, 
jeste taj što je Pig Latin proceduralni jezik visokog nivoa. Kao i većina programera, zagovornik sam proceduralnog
programiranja i nisam ljubitelj deklarativnih jezika koji liče na SQL. Za detaljniji pregled Apache Pig alata
može se pogledati sledeći [rad](https://drive.google.com/open?id=1--N_Dvrt1SCL5IKN83nTgv19ugZmDvbY),
 kao i [Apache Pig dokumentacija](https://pig.apache.org/).

## Analiza skupa podataka
Neke od činjenica koje su analizirane u ovom projektu su:
- na koje kompanije se korisnici najviše žale ([complaints-per-company.pig](https://github.com/vladaindjic/paketna-obrada-ASVSP/blob/master/opisane-skripte/complaints-per-company.pig))
- na koje proizvode se korisnici najviše žale ([complaints-per-product.pig](https://github.com/vladaindjic/paketna-obrada-ASVSP/blob/master/opisane-skripte/complaints-per-product.pig))
- da li kompanije u propisanom roku odgovaraju na žalbe ([complaints-per-timelyResponse.pig](https://github.com/vladaindjic/paketna-obrada-ASVSP/blob/master/opisane-skripte/complaints-per-timelyResponse.pig))
- od svih žalbi jedne kompanije, koliki je procenat žalbi na kreditne kartice ([credit-card.pig](https://github.com/vladaindjic/paketna-obrada-ASVSP/blob/master/opisane-skripte/credit-card.pig))
- putem kojih sredstava za komunikaciju se žalbe najčešće podnošenja ([complaints-per-submittedVia.pig](https://github.com/vladaindjic/paketna-obrada-ASVSP/blob/master/opisane-skripte/complaints-per-submittedVia.pig))
- koliko je prosečno vreme dostavljanja žalbe kompanije u zavisnosti od sredstva podnošenja ([response-time.pig](https://github.com/vladaindjic/paketna-obrada-ASVSP/blob/master/opisane-skripte/response-time.pig))
- na koliki procenat žalbi kompanija odgovori sa zakašnjenjem ([late-response-percentage.pig](https://github.com/vladaindjic/paketna-obrada-ASVSP/blob/master/opisane-skripte/late-response-percentage.pig))
- koji vid komunikacije stariji Amerikanci najviš koriste za podnošenje žalbi ([older-people.pig](https://github.com/vladaindjic/paketna-obrada-ASVSP/blob/master/opisane-skripte/older-people.pig))

## Rezultati analize
Sažeti rezultati analize dati su u komentarima skripti, kao i u [PDF dokumentu](). Za detaljne, potrebno je pokrenuti dogovarajuću skriptu i 
videti rezultat njenog izvršavanja na HDFS-u.