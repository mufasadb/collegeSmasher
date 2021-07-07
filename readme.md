Expects file structure where the daily "zips" are being unzipped, renamed "files" and then placed into this root dir.

should be files -> college -> apiOutput csv
example: 
files/acap/campus.csv



files come out into the folder labeled output


To collate each csv into one across all colleges: 

npm run smash 

this also outputs a total for api call status' into a total JSON
if you also want the total csv 

npm run countTotal


