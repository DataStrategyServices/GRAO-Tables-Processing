#Data For Good: GRAO Tables Processing
## Updating WikiData properties (settlement population in Bulgaria)

##	Description
Update Wikidata information regrading the permanent and current population of Bulgarian settlements every quarter.  
Using GRAO *https://www.grao.bg/* data that is generated every three months (15th day of the month);  
NSI (National statistical institute) data;
Wikidata query for code extraction - *https://query.wikidata.org/sparql*;  
Pywikibot to reset ranks for each value to normal, so that most recent values remain at preferred.
WikiBot for uploading data to Wikidata (Wikipedia) - the upload takes approximately 5 hours for each quarter.


##	Installation
> Clone the repo:  
>> git clone [url]

> Make sure you have credentials for WikiData Bot in order your upload to be successful.  
Deal with credential storing in whatever way you see fit, but it is adisable not to upload
credential files to online codesharing websites, or to simply hardcode the username/password.

> Technologies:
>> Python 3.9 ; 
>> Sparql - The query needed is already available in Python script;

### External Libraries:  
> **pywikibot**
>>1. git clone https://github.com/wikimedia/pywikibot.git --depth 1
>>2. In Command Prompt
>>3. python -m pip install -U setuptools
>>4. python -m pip install mwparserfromhell
>>5. python -m pip install -e pywikibot/
>>6. cd pywikibot/
>>7. python pwb.py generate_user_files.py
>>8. username*
>>9. bot_login_name*
>>10. bot_password*
>>11. *these need to be configured by the user
>>12. The bot produces a user-config.py and user-password.py
these both need to be included in the project package, with the proper authentication.

> **wikidataintegrator**  
>> 1. git clone https://github.com/sebotic/WikidataIntegrator.git
>> 2. cd WikidataIntegrator
>> 3. python setup.py install

> Additional information:
>> 1. Follow the steps in a Windows cmd in order to successfully install the libraries.
>> 2. Following installation, the pywikibot will require credentials that need to also be dealt with.
>> 3. It uses two files - user-config.py and password.py, those files contain information on the login name, as well as throttling.
>> 4. It is required to test whether throttling is needed for your project, in our case it was sufficient to set throttling to 0.

>
> Check **requirements.txt** for further information.


##	Usage



##	Contributing

The script is separated into 7 modules:
###1. *acquire_url*
- Checks the most recent uploaded report;
- Generates the link for the current report;
- Keeps a log file of completed reports and errors;
###2. *markdown_to_df*  
- Extracts Markdown Table (GRAO) from generated URL;  
- Converts to DataFrame;  
- Cleans and transforms the data;  
###2. *ekatte_dataframe*  
- Extracts NSI(National statistical institute) data
- Converts to DataFrame;  
- Cleans and transforms the data; 
- Merges with the GRAO Dataframe; 
###3. *wikidata_codes*  
- Extracts data from wikidata (*https://query.wikidata.org/sparql*);    
- Cleans and transforms the data partially;
- Merges with the combined ekatte and GRAO dataframe;
###4. *wikidata_uploader*  
- Pywikibot implementation to reset old ranks to normal;
- Set most recent values to "preferred" rank;
- WikiData Integrator integration;
- Uploads the population data to the respective settlement properties;
- Logs errors and failures to upload, as well as bot activity.

##	Project Status:
- In Development
