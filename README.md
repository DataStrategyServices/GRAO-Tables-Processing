#	Data For Good: GRAO Tables Processing

##	Description
Project to update Wikidata information regrading the permanent and current population of Bulgarian
settlements every quarter.

We use the following resources:

* Using GRAO <https://www.grao.bg/> data that is generated every three months (15th day of the month);
* NSI (National statistical institute) data;
* Wikidata query for code extraction - <https://query.wikidata.org/sparql>;
* Pywikibot to reset ranks for each value to normal, so that most recent values remain at preferred
* WikiBot for uploading data to Wikidata (Wikipedia) - the upload takes approximately 5 hours for each quarter


##	Installation
Clone the repo:  

```bash
git clone [url]
```

Make sure you have credentials for WikiData Bot in order your upload to be successful. Deal with
credential storing in whatever way you see fit, but it is advisable not to upload credential files to
online codesharing websites, or to simply hard-code the username/password in the code.

Technologies:

* Python>=3.9
* SPARQL - The query needed is already available in Python script;


### External Libraries:  
Some dependencies require additional manual installation steps and as such we cannot rely on the
provided `requirements.txt` file directly.

Overview:

1. Follow the steps in your preferred command line program in order to successfully install the
   libraries.
1. After the installation, pywikibot will require credentials that need to also be dealt with.
1. Pywikibot uses two files - `user-config.py` and `password.py`, those files contain information on
   the login name, as well as throttling.
1. You should test whether throttling is needed for your project, in our case it was sufficient to
   set throttling to 0.

**pywikibot** - run the following commands in your preferred command line application

```
git clone https://github.com/wikimedia/pywikibot.git --depth 1`
pip install -U setuptools`
pip install mwparserfromhell
pip install -e pywikibot/
cd pywikibot/
python pwb.py generate_user_files.py
<username*>
<bot_login_name*>
<bot_password*>
```

\*these fields need to be input by the user

The above bot installation script produces a `user-config.py` and `user-password.py` these both need
to be included in the project package, with the proper authentication. For security reasons these
files are included in the repository's `.gitignore` file.

**wikidataintegrator**  

```
git clone https://github.com/sebotic/WikidataIntegrator.git
cd WikidataIntegrator
python setup.py install
```

Finally, install all remaining requirements using `pip` and the provided `requirements.txt` file:

```
pip install -r requirements.txt
```

##	Usage
The project is structured as a runnable Python package. In order to start the loading process, call
the module from the project's root folder:

```
cd <project path>
python -m grao_table_processing
```


##	Contributing
The script is separated into 5 modules:


### 1. *acquire_url*

- Checks the most recent uploaded report;
- Generates the link for the current report;
- Keeps a log file of completed reports and errors;


### 2. *markdown_to_df*  

- Extracts Markdown Table (GRAO) from generated URL;  
- Converts to DataFrame;  
- Cleans and transforms the data;  


### 3. *ekatte_dataframe*  

- Extracts NSI(National statistical institute) data
- Converts to DataFrame;  
- Cleans and transforms the data; 
- Merges with the GRAO Dataframe; 


### 4. *wikidata_codes*  

- Extracts data from wikidata (<https://query.wikidata.org/sparql>);
- Cleans and transforms the data partially;
- Merges with the combined EKATTE and GRAO dataframe;


### 5. *wikidata_uploader*  

- Pywikibot implementation to reset old ranks to normal;
- Set most recent values to "preferred" rank;
- WikiData Integrator integration;
- Uploads the population data to the respective settlement properties;
- Logs errors and failures to upload, as well as bot activity.


##	Project Status
- [ ] Planning
- [ ] Prototype
- [ ] In Development
- [X] In Production
- [ ] Unsupported
- [ ] Retired


##	Authors and Acknowledgement
Project Maintainer:

- [Dimitar Atanasov](https://github.com/Hear7y)

Development Team:

- [Dimitar Atanasov](https://github.com/Hear7y)
- [Ivelina Balcheva](https://github.com/rolaart)
- [Nikolay Nikolov](https://github.com/nkonstnikolov)
- Stanislav Spasov
- Valentina Yordanova

Consultation:

- [Radoslav Dimitrov](https://github.com/Bugzey)

Sponsor:

- [Data for Good Bulgaria](https://data-for-good.bg/) ([Github page](https://github.com/data-for-good-bg))

README based on <https://www.makeareadme.com/>

