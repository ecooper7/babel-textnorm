# babel-textnorm
Text normalization module for data scraped from the web.

contact: ecooper@cs.columbia.edu


1.  Dependencies
    This normalization tool requires NLTK for Python.
    On Ubuntu, it can be installed using:
       sudo apt-get install python-nltk
    This script has been tested using Python 2.7.6.


2.  Running Normalization on Existing Data

    - These scripts assume that your data is in a directory structure like the following:
    
      data 
        bbn
          webtext_202
            00
              [all files extracted into .txt format]
            01
            02
            ...
          webtext_205
          ...
        columbia
          202
            blogspot
            TED
          205
            news
          ...
        subtitles
          202
            [All the xml files for that language, from the 'xml' directory, not 'raw'.
             These must be first extracted from their .tar.gz format and put together.]
          302
          ...
    
    - Once the data is there, update lc_ to pick which language you are processing.
    - Also update file_base_ to point to your data directory.
    - Run normalizer.py.  
      The normalized output will be in files [language code]_[genre].txt.
    

3.  Adding Support for Normalizing a New Language

    - Add any collected web data to the appropriate directories.
    - Update lc_ with your new language code.
    - Update genres_ to include the names of any genres for which you have data.
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
    - If the language uses a non-Latin alphabet, you can optionally add it to 
      the first 'if' statement in post_normalization, to have it skip any 
      sentences that include Latin.  This is just ad-hoc language filtering.


4.  Notes on Sentence Tokenization

    - We are using the Punkt sentence tokenizer (distributed as part of NLTK).  
      This method of tokenization is unsupervised, language-independent, and 
      character set independent.
      See "Unsupervised Multilingual Sentence Boundary Detection" 
      (Tibor Kiss and Jan Strunk, Computational Linguistics, 2006).
    - For the Columbia web data, we are training the tokenizer on all genres
      together, because for some genres in some languages, we don't have 
      enough data to learn genre-specific tokenizers (e.g. Cebuano TED talks,
      for which only one document was found, containing 851 words.)
    - For the BBN web data, we are only training the tokenizer on files in 
      subdirectories starting with '0' (1/16th of the data).  This is because 
      training on all of the BBN web data would take too long, we believe 
      that the data is uniform enough that training on the entire data would not 
      substantially improve the tokenization, and the slice of the data we are
      using is an appropriate representative sample.
    - For the subtitles, we are not using the Punkt tokenizer at all.  This is 
      because the XML format provides sentence segmentation for us already.
    - While making this script, we noticed that running normalization on
      Columbia web data and BBN web data separately produced different results
      than running them in sequence, at the sentence tokenization level, even 
      though separate tokenizers are created and trained for each set.  Since 
      we were running them separately for the evaluation and only merged them 
      to clean up the code for distribution, it is necessary to reload the punkt
      module each time we make a new tokenizer, to ensure consistency with our
      original evaluation output.
