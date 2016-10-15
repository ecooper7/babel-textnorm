# babel-textnorm
Text normalization module for data scraped from the web.

contact: ecooper@cs.columbia.edu


1.  Dependencies
    This normalization tool requires NLTK for Python.
    On Ubuntu, it can be installed using:
       sudo apt-get install python-nltk
    This tool also expects data to be present in a MongoDB, and uses the pymongo library.
    This script has been tested using Python 2.7.6.


2.  Running Normalization on Existing Data

    - These scripts assume that your data is in a directory structure similar to what was provided for the surprise language evaluation.  You will need to add your language and file paths to sources_, and also update the webroot variable to point to the base directory for the data.  This applies to BBN webdata, subtitles, ted, and wiki.
    - The data collected by the Columbia web scraping tools is expected to be in a database called 'scraping'.  For the eval it was provided in json format; to use this normalization tool the json files must be already loaded into the database.
    
    - Once the data is there, update lc_ to pick which language you are processing.
    - Also update mongo_lc_ and mongo_genres_ if necessary.
    - Run normalizer.py.  
      The normalized output will be in files [language code]_[genre].txt.
    

3.  Adding Support for Normalizing a New Language

    - Add any collected web data to the appropriate directories.
    - Update lc_ with your new language code.
    - If the language does not use the Latin character set, add it to the non_latin_ list.
    - Update numeral_regex_ for your language.  
      Most languages just use latin_numeral_regex, but e.g. Telugu has some of 
      its own numeral characters.
    - Update table_of_numbers_ to allow expansion of numbers into words.
      We have populated this from information found in the LSP.  
      When we were unsure about how to expand a number, we erred on the side of
      caution and just left it.  Numbers that don't get expanded later get 
      converted to the token <NUM>.
    - Update charset_regex_ to describe the alphabet of your language.
      These are as defined in the LSP.  They should not include punctuation or
      numerals.


4.  Notes on Sentence Tokenization

    - We are using the Punkt sentence tokenizer (distributed as part of NLTK).  
      This method of tokenization is unsupervised, language-independent, and 
      character set independent.
      See "Unsupervised Multilingual Sentence Boundary Detection" 
      (Tibor Kiss and Jan Strunk, Computational Linguistics, 2006).
    - For the BBN web data, we are only training the tokenizer on files in 
      subdirectories starting with '0' (1/16th of the data).  This is because 
      training on all of the BBN web data would take too long, we believe 
      that the data is uniform enough that training on the entire data would not 
      substantially improve the tokenization, and the slice of the data we are
      using is an appropriate representative sample.
    - For the subtitles, we are not using the Punkt tokenizer at all.  This is 
      because the XML format provides sentence segmentation for us already.
      There are files in both the 'xml' directory and the 'raw' directory.  
      This is the same data in different formats.  The normalizer works on the 
      data in the 'xml' directory.  This data is assumed to have been already 
      extracted from .tar.gz, and put all together in one place.
    - For the wiki data, we are also not using the Punkt tokenizer because the
      data appears to have been provided in an already-tokenized form.
