#!/usr/bin/python
# coding=utf-8

__author__ = 'victor,erica'

import codecs
import os
from nltk.tokenize import punkt
import re
import xml.etree.ElementTree as ET

# which language are we processing right now?
lc_ = '202'

# where's the data?
file_base_ = '/local2/ecooper/normalizer/data/'

# which genres do we have for each language?
genres_ = { '202' : ['bbn', 'TED', 'blogspot', 'subtitles'],
            '205' : ['bbn', 'news'],
            '207' : ['bbn', 'blogspot'],
            '301' : ['bbn', 'TED', 'blogspot'],
            '302' : ['bbn', 'TED', 'blogspot', 'subtitles'],
            '303' : ['bbn', 'TED', 'news', 'blogspot', 'subtitles'],
            '304' : ['bbn', 'TED', 'blogspot', 'subtitles'],
           }

tokenizers_ = {}

# 20-22 is space, quotation mark and exclamation mark
# 27 is apostrophe
# 2c-2e is comma, hyphen and full stop
# 30-3B is digits 0-9 and : ;
# 3f is question mark
# 41-5a is capital latin letters A-Z
# 5f is underscore
# 61-7a is small latin letters a-z
newline_regex_ = u'\u000a\u000b\u000c\u000d\u0085\u2028\u2029'
space_regex_ =  u'\u0009\u0020\u00a0\u2000-\u200a\u202f\u205f\u3000'
whitespace_regex_ = newline_regex_ + space_regex_
latin_alphabet_regex_ = u'\u0041-\u005a\u0061-\u007a'
latin_numeral_regex_ = u'\u0030-\u0039'
latin_punct_regex_ = u'\u0021\u0022\u0027\u002c\u002d\u002e\u003a\u003b\u003f\u005f'
latinset_regex_ = whitespace_regex_ + latin_alphabet_regex_ + latin_numeral_regex_ + latin_punct_regex_
eos_regex_ = u'\u002e\u003f\u0021'

# 202 (Swahili) uses the basic Latin alphabet.

# 205 (Kurmanji) uses the basic Latin characters 41-7A
#  (notice that this interval contains [ \ ] ^ _ ` , those have been removed)
# plus 10 additional characters from LATIN_EXTENDED_A

# 207 (Tok Pisin) uses a subset of the Latin alphabet a-z
# except: q,x,z. c is only used in names and abbreviations
# but we'll keep it because it still shows up sometimes (e.g. loan words)

# 301 (Cebuano) the Cebuano alphabet is a subset of the Latin-script
# English alphabet. It is comprised of 19 letters.
# For foreign words, which are unavoidably used in the language,
# foreign letters are utilized. These include <Cc>, <Ff>, <Jj>, <Ññ>, <Qq>,
# <Vv>, <Xx>, <Zz>. These characters are adopted to reflect foreign
# pronunciations and spellings, but they do not form an integral part of the
# native alphabet. Here we allow a-z A-Z Ñ ñ

# 302 (Kazakh) the LP states that Kazakh's alphabet is composed of 42 letters
# from the Cyrillic alphabet (full Russian alphabet + 3 Kazakh characters),
# but then gives the unicode range 0400-04ff, which contains more than 42 letters.
# The Kazakh specific interval is 0410-044f plus 0451, 0456, 0492, 0493, 049a,
# 049b, 04a2, 04a3, 04ae, 04af, 04b0, 04b1, 04ba, 04bb, 04d8, 04d9, 04e8, 04e9,
# 0401, 0406
# There's also the exception that "Please note that for English and other
# European proper nouns that occur in the data, the standard Latin scripted
# character set (U+0041 – U+007a) is used"

# 303 (Telugu) The Unicode range for Telugu is U+0C00 – U+0C7F
# There are also some Telugu-specific numeral characters.

# 304 (Lithuanian) Lithuanian uses the Basic Latin Unicode range (U+0041-U+007A)
# plus the following additional forms from the Latin Extended-A set:
# U+0104, U+0105, U+010c, U+010d, U+0116, U+0117, U+0118, U+0119, U+012e, U+012f
# U+0160, U+0161, U+016a, U+016b, U+0172, U+0173, U+017d, U+017e

numeral_regex_ = {'202': latin_numeral_regex_,
                  '205': latin_numeral_regex_,
                  '207': latin_numeral_regex_,
                  '301': latin_numeral_regex_,
                  '302': latin_numeral_regex_,
                  '303': latin_numeral_regex_ + u'\u0c66-\u0c6f\u0c78-\u0c7f',
                  '304': latin_numeral_regex_,
                  }

# from the LSP
table_of_numbers_ = {
    '202' : {
        # 0 is either sifuri or sufuri, not sure when you'd use which
        '1' : u'moja',
        '2' : u'mbili',
        '3' : u'tatu',
        '4' : u'nne',
        '5' : u'tano',
        '6' : u'sita',
        '7' : u'saba',
        '8' : u'nane',
        '9' : u'tisa',
        '10' : u'kumi',
        '20' : u'ishirini',
        '30' : u'thelathini',
        #'40' : u'', arubaini / arobaini
        '50' : u'hamsini',
        '60' : u'sitini',
        '70' : u'sabini',
        '80' : u'themanini',
        '90' : u'tisini',
        #'100' : u'', mia / mia moja
        #'1000' : elfu / elfu moja
        '10000' : u'elfu kumi',
        #'100000' : laki / laki moja
        '10000000' : u'milioni kumi',
        },
    '205' : {
        '0' : u'sifir',
        '1' : u'yek',
        '2' : u'du',
        '3' : u's\u00ea',
        '4' : u'\u0037ar',
        '5' : u'p\u00eanc',
        '6' : u'\u015fe\u015f',
        '7' : u'heft',
        '8' : u'he\u015ft',
        '9' : u'neh',
        '10' : u'deh',
        '11' : u'yazdeh',
        '12' : u'duwazdeh',
        '13' : u's\u00eazdeh',
        '14' : u'\u00e7ardeh',
        '15' : u'pazdeh',
        '16' : u'\u015fazdeh',
        '17' : u'hivdeh',
        '18' : u'hijdeh',
        '19' : u'nozdeh',
        '20' : u'b\u00eest',
        '30' : u'sih',
        '40' : u'\u00e7il',
        '50' : u'p\u00eanc\u00ee',
        '60' : u'\u015f\u00east',
        '70' : u'heft\u00ea',
        '80' : u'he\u015ft\u00ea',
        '90' : u'nod',
        '100' : u'sed',
        '1000' : u'hezar',
        '10000' : u'deh hezar',
        '100000' : u'sed hezar',
        '1000000' : u'milyon',
        },
    '207' : {
        '0' : u'siro',
        '1' : u'wan',
        '2' : u'tu',
        '3' : u'tri',
        '4' : u'foa',
        '5' : u'faiv',
        '6' : u'sikis',
        '7' : u'seven',
        '8' : u'et',
        '9' : u'nain',
        '10' : u'ten',
        '11' : u'ileven',
        '12' : u'twelv',
        '13' : u'tetin',
        '14' : u'fotin',
        '15' : u'fiftin',
        '16' : u'sikstin',
        '17' : u'seventin',
        '18' : u'etin',
        '19' : u'naintin',
        '20' : u'twenti',
        '100' : u'wan handet',
        '1000' : u'wan tausen',
        '10000' : u'ten tausen',
        '100000' : u'wan handet tausen',
        '1000000' : u'wan milien',
        '10000000' : u'ten milien',
        },
    '301' : {
        # Cebuano - there's Cebuano and Spanish-type numbers and
        # it's not clear which to use. Leaving empty rather than guessing.
        },
    '302' : {
        #'0' : u'', has two variants; not sure when to use which.
        '1' : u'\u0431\u0456\u0440',
        '2' : u'\u0435\u043a\u0456',
        '3' : u'\u04af\u0448',
        '4' : u'\u0442\u04e9\u0440\u0442',
        '5' : u'\u0431\u0435\u0441',
        '6' : u'\u0430\u043b\u0442\u044b',
        '7' : u'\u0436\u0435\u0442\u0456',
        '8' : u'\u0441\u0435\u0433\u0456\u0437',
        '9' : u'\u0442\u043e\u0493\u044b\u0437',
        '10' : u'\u043e\u043d',
        '20' : u'\u0436\u0438\u044b\u0440\u043c\u0430',
        '30' : u'\u043e\u0442\u044b\u0437',
        '40' : u'\u049b\u044b\u0440\u044b\u049b',
        '50' : u'\u0435\u043b\u0443',
        '60' : u'\u0430\u043b\u043f\u044b\u0441',
        '70' : u'\u0436\u0435\u0442\u043f\u0456\u0441',
        '80' : u'\u0441\u0435\u043a\u0441\u0435\u043d',
        '90' : u'\u0442\u043e\u049b\u0441\u0430\u043d',
        '100' : u'\u0436\u04af\u0437',
        '1000' : u'\u043c\u044b\u04a3',
        '10000' : u'\u043e\u043d \u043c\u044b\u04a3',
        '100000' : u'\u0436\u04af\u0437 \u043c\u044b\u04a3',
        '10000000' : u'\u043e\u043d \u043c\u0438\u043b\u043b\u0438\u043e\u043d',
        },
    '303' : {
        '0' : u'\u0c38\u0c41\u0c28\u0c4d\u0c28\u0c3e',
        u'\u0c66' : u'\u0c38\u0c41\u0c28\u0c4d\u0c28\u0c3e',
        '1' : u'\u0c12\u0c15\u0c1f\u0c3f',
        u'\u0c67' : u'\u0c12\u0c15\u0c1f\u0c3f',
        '2' : u'\u0c30\u0c46\u0c02\u0c21\u0c41',
        u'\u0c68' : u'\u0c30\u0c46\u0c02\u0c21\u0c41',
        '3' : u'\u0c2e\u0c42\u0c21\u0c41',
        u'\u0c69' : u'\u0c2e\u0c42\u0c21\u0c41',
        '4' : u'\u0c28\u0c3e\u0c32\u0c41\u0c17\u0c41',
        u'\u0c6a' : u'\u0c28\u0c3e\u0c32\u0c41\u0c17\u0c41',
        '5' : u'\u0c10\u0c26\u0c41',
        u'\u0c6b' : u'\u0c10\u0c26\u0c41',
        '6' : u'\u0c06\u0c30\u0c41',
        u'\u0c6c' : u'\u0c06\u0c30\u0c41',
        '7' : u'\u0c0f\u0c21\u0c41',
        u'\u0c6d' : u'\u0c0f\u0c21\u0c41',
        '8' : u'\u0c0e\u0c28\u0c3f\u0c2e\u0c3f\u0c26\u0c3f',
        u'\u0c6e' : u'\u0c0e\u0c28\u0c3f\u0c2e\u0c3f\u0c26\u0c3f',
        '9' : u'\u0c24\u0c4a\u0c2e\u0c4d\u0c2e\u0c3f\u0c26\u0c3f',
        u'\u0c6f' : u'\u0c24\u0c4a\u0c2e\u0c4d\u0c2e\u0c3f\u0c26\u0c3f',
        '10' : u'\u0c2a\u0c26\u0c3f',
        u'\u0c67\u0c66' : u'\u0c2a\u0c26\u0c3f',
        '100' : u'\u0c35\u0c02\u0c26',
        u'\u0c67\u0c66\u0c66' : u'\u0c35\u0c02\u0c26',
        '1000' : u'\u0c35\u0c46\u0c2f\u0c4d\u0c2f\u0c3f',
        u'\u0c67\u0c66\u0c66\u0c66' : u'\u0c35\u0c46\u0c2f\u0c4d\u0c2f\u0c3f',
        '10000' : u'\u0c2a\u0c26\u0c3f \u0c35\u0c47\u0c32\u0c41',
        u'\u0c67\u0c66\u0c66\u0c66\u0c66' : u'\u0c2a\u0c26\u0c3f \u0c35\u0c47\u0c32\u0c41',
        '100000' : u'\u0c32\u0c15\u0c4d\u0c37',
        u'\u0c67\u0c66\u0c66\u0c66\u0c66\u0c66' : u'\u0c32\u0c15\u0c4d\u0c37',
        #'1000000' : u'\u0c15\u0c4b\u0c1f\u0c3f/\u0c2a\u0c26\u0c3f \u0c2e\u0c3f\u0c32\u0c3f\u0c2f\u0c28\u0c4d\u0c32\u0c41'
        #u'\u0c67\u0c66\u0c66\u0c66\u0c66\u0c66\u0c66\u0c66' : u'\u0c15\u0c4b\u0c1f\u0c3f/\u0c2a\u0c26\u0c3f \u0c2e\u0c3f\u0c32\u0c3f\u0c2f\u0c28\u0c4d\u0c32\u0c41'
        },
    '304' : {
        # Lithuanian numbers have case which means that the word might change
        # depending on what it's describing.  I don't think we can confidently
        # replace them so leaving this empty.
        }
    }

# as defined by the language packs
# basically the alphabet of the language.
# These do not include punctuation or numerals.
charset_regex_ = {'202' : latin_alphabet_regex_ + u'\u00e9\u00e1',
                  '205': u'\u0041-\u005a\u0061-\u007a\u00c7\u00ca\u00ce\u00db\u015e\u00e7\u00ea\u00ee\u00fb\u015f',
                  '207': latin_alphabet_regex_,
                  '301': u'\u0041-\u005a\u0061-\u007a\u00d1\u00f1',
                  '302': u'\u0400-\u04ff\u0451\u0456\u0492\u0493\u049a\u049b\u04a2\u04a3\u04ae\u04af\u04b0\u04b1\u04ba\u04bb\u04d8\u04d9\u04e8\u04e9\u0401\u0406',
                  '303': u'\u0c00-\u0c7f\u200c',
                  '304': u'\u0041-\u005a\u0061-\u007a\u0104\u0105\u010c\u010d\u0116\u0117\u0118\u0119\u012e\u012f\u0160\u0161\u016a\u016b\u0172\u0173\u017d\u017e'}

########## REGEX FUNCTIONS ##########

def replace_ellipses(text):
    ellipsis_regex = u'\u2026|(\.\.+)|(\s\.(\s\.)+)' ## >1 period
    # also in Tok Pisin: this is an ellipsis . . .
    # Replace ellipsis with space or EOL after it with '. '
    res = re.sub(ellipsis_regex + r'[' + whitespace_regex_ + '$]', '. ', text)
    # if it's an ellipsis between words...like this - then replace with a space
    # if it's an ellipsis before a word ...like this - then replace with space
    res = re.sub(ellipsis_regex, ' ', text)
    return res

non_sentences_ = []

def fix_lists_and_titles(text, genre):
    if genre == 'bbn':
        #for BBN web data; if lone sentence w/o EOS punct,
        # remove it. (not proper sentences)
        # generally: if we're not sure what it is, then we don't keep it.
        parts = [x.strip() for x in text.split('\n')]
        res = ''
        for p in parts:
            if len(p) == 0:
                continue
            if p[-1] in eos_regex_ + '"':
                res += p + '\n'
        return res
    else:
        # for bulleted lists and titles:
        # remove them. (not proper sentences)
        r = re.search(r'((^)|(\n\n)).*[^'+eos_regex_+']\n\n+', text)
        if r:
            text = re.sub(r'^.*[^'+eos_regex_+']\n\n+', '\n\n', text)
        # remove bullet lists indicated by dashes
        r = re.search(r'^\s*\- .*[^'+eos_regex_+']\n', text)
        if r:
            text = re.sub(r'\- .*[^'+eos_regex_+']\n', '\n', text)
        return text
    
def fix_whitespace(text):
    # standardize newlines
    fixed = re.sub(r'[' + newline_regex_ + ']+', '\n', text)
    # standardize spaces
    return re.sub(r'[' + space_regex_ + ']+', ' ', fixed)

def fix_hyphens(text, lang_code):
    # standardize hyphens
    text = re.sub(u'\u2013', '-', text)
    # remove any hyphens that are not part of a hyphated word,
    # i.e. surrounded by Language-alphabet characters.
    # (also allow numerals for e.g. scientific names for things)
    l_non_word_hyphen = r'(?<![' + charset_regex_[lang_code]+numeral_regex_[lang_code] + '])-'
    r_non_word_hyphen = r'-(?![' + charset_regex_[lang_code]+numeral_regex_[lang_code] + '])'
    text = re.sub(r_non_word_hyphen, ' ', text)
    return re.sub(l_non_word_hyphen, ' ', text)

# standardize apostrophes
def fix_apostrophes(text, lang_code):
    weird_apostrophe_regex = u'\u2018\u2019\u201c\u201d\u02bc\u02bb\u055a\ua78b\ua78c\uff07\u00b4\u0022\u0060'
    text = re.sub(r'[' + weird_apostrophe_regex + ']', "'", text)
    # also only allow word-internal apostrophes.
    # (same as with hyphens)
    l_non_word_apostrophe = r'(?<![' + charset_regex_[lang_code]+numeral_regex_[lang_code] + "])'"
    r_non_word_apostrophe = r"'(?![" + charset_regex_[lang_code]+numeral_regex_[lang_code] + '])'
    text = re.sub(r_non_word_apostrophe, ' ', text)
    return re.sub(l_non_word_apostrophe, ' ', text)

def fix_punct(text, lang_code):
    # handle nonstandard punctuation usage.
    # period after space and before a letter:
    # put space after the period and remove the space before.
    # this applies to .,?!:;
    punct_to_fix = '\.,\?!:;'
    res = re.sub(' (['+punct_to_fix+'])(?=[' + charset_regex_[lang_code] + '])', r'\1 ', text)
    # space before and after a period: remove the space before.
    res = re.sub(' (['+punct_to_fix+']) ', r'\1 ', res)
    # same if it's end of line instead of space
    res = re.sub(' (['+punct_to_fix+'])$', r'\1', res)
    return res

def replace_everything_else(text, lang_code):
    # replace anything not in latin or in our language with space
    pre_regex = r'[^' + latinset_regex_ + charset_regex_[lang_code] + ']+'
    return re.sub(pre_regex, ' ', text)

def fix_sentences(text, lang_code):
    eos_regex = eos_regex_ + u','
    alph = charset_regex_[lang_code]
    r = r'(?<=[' + alph + '])' + '([' + eos_regex + ']+)' + '(?=([' + alph + ']{2,}))'
    return re.sub(r, r'\1 ', text)

def remove_urls(text):
    if re.search('(http://)|(www\.)', text):
        return ''
    return text

def fix_abbrevs(text):
    # replace periods with underscores if they appear within a word rather than
    # at the end of a sentence. since this is post-tokenization, the sentence
    # tokenizer has already presumably figured out which are sentence endings
    # and which are abbrevs.
    if re.search(r'\.(?!$)', text.strip()):
        res = re.sub(r'\.(?!$)', u'\u005f', text.strip())
        return res
    else:
        return text

def remove_stuff(text, lang_code):
    # Remove everything that's not a numeral or an alphabet character in the
    # language.  Allow hyphens.  Allow underscores.  Also allowing apostrophes.
    # This removes Latin alphabet characters and punctuation.
    # Also leave in underscores - assume we already fixed abbrevs with underscores.
    # replace commas with empty string (e.g. for numbers)
    res = re.sub(',', '', text)
    keep_chars = u'\u005f' + numeral_regex_[lang_code] + charset_regex_[lang_code] + '\-' + "'"  
    res = re.sub(r'[^' + keep_chars + r']', ' ', res)
    return res

def fix_nums(text, lang_code):
    # replace any word containing numerals with <NUM>.
    # or with the spelled-out word if we have it in our table.
    nums = table_of_numbers_[lang_code]
    res = ''
    toks = text.split()
    for t in toks:
        if re.search(r'[' + numeral_regex_[lang_code] + r']', t):
            if t in nums.keys():
                t = nums[t]
            else:
                t = '<NUM>'
        res += t + ' '
    return res

########## NORMALIZATION SUBROUTINES ##########

# First round of fixes
# The things you can fix before sentence segmentation
def pre_normalization(filename, lang_code, genre):
    with codecs.open(filename, 'r', encoding='utf-8') as f:
        unnormalized_text = f.read().strip()
        if unnormalized_text == '':
            return ''
        # If no EOS punctuation at the EOF, put one
        # but if there's some other punctuation then it's okay.
        if unnormalized_text[-1] not in eos_regex_ + latin_punct_regex_:
            unnormalized_text += '.'
        res = fix_lists_and_titles(unnormalized_text, genre)
        res = replace_ellipses(res)
        res = fix_whitespace(res)
        res = fix_hyphens(res, lang_code)
        res = fix_apostrophes(res, lang_code)
        res = replace_everything_else(res, lang_code)
        res = fix_punct(res, lang_code)
        # remove underscores in the original text
        res = re.sub('_', ' ', res)
        # fix periods in e.g. Tok Pisin - no spaces between sentences sometimes.
        # heuristic: if you have a period followed by >1 alphabet char,
        # insert a space.
        if lang_code == '207': # just Tok Pisin for now
            res = fix_sentences(res, lang_code)
        return res
    
# Return a trained tokenizer
# Train a new one if you don't have one already
# For bbn web data, just train on the first slice of the data.
def get_trained_tokenizer(lang_code, prenorm_text_dict, bbn):
    if (bbn != '0' and bbn != '-1'):
        print 'USING TRAINED TOKENIZER'
        return tokenizers_['bbn']
    else:
        print 'TRAINING NEW TOKENIZER'
        global punkt
        punkt = reload(punkt)
        tokenizer = punkt.PunktSentenceTokenizer()
        text = ''
        for genre, prenorm_text in prenorm_text_dict.iteritems():
            text += prenorm_text + '\n'
        params = tokenizer.train(text)
        if bbn == '0':
            print 'SETTING BBN TOKENIZER'
            tokenizers_['bbn'] = tokenizer
        else:
            print 'SETTING CU TOKENIZER'
            tokenizers_['cu'] = tokenizer
        print 'DONE TRAINING TOKENIZER'
        return tokenizer

def post_normalization(s, lang_code):
    # skip any sentences that contain Latin alphabet.
    # this is presumed to include URLs.
    # disallow Latin in Telugu and Kazakh.
    if lang_code == '303' or lang_code == '302':
        if re.search(r'[' + latin_alphabet_regex_ + ']', s):
            return ''
    # various fixes
    res = remove_urls(s)
    res = fix_abbrevs(res)
    res = remove_stuff(res, lang_code)
    res = fix_nums(res, lang_code)
    # collapse <NUM> <NUM> 
    res = re.sub('(<NUM> )+', '<NUM> ', res)
    # replace any multiple whitespace with single whitespace
    res = re.sub(r'\s+', ' ', res)
    # replace any multiple underscores with single underscore
    # these generally result from punctuation typos in the source text
    res = re.sub(r'_+', '_', res)
    # done
    return res.strip()

### Normalizer specifically for subtitles.
### Just need to parse out the XML and clean up any out-of-language text.
### It's already in utterances, so no need to tokenize.
### Punctuation is already separated out, mostly.
def normalize_subtitles(lang_code):
    outf = codecs.open(lang_code + '_subtitles.txt', 'w', encoding='utf-8')
    # 1. Parse out the sentences
    base = file_base_ + 'subtitles/' + lang_code
    files = os.listdir(base)
    for f in files:
        fname = base + '/' + f
        print fname
        tree = ET.parse(fname)
        root = tree.getroot()
        for s in root:  # <s id="1"> etc. - the utterances
            sentence = ''
            for w in s.findall('w'): # the words
                t = w.text.strip()
                t = re.sub(r'\|', '', t)
                t = t.strip(',')
                t = t.strip('_')
                # get rid of the <i> </i> things in Telugu
                if re.search(r'[<>/;]', t):
                    continue
                # if Latin script in Telugu text, <foreign>
                # also allow for Kazakh because we know it shouldn't have Latin
                if lang_code in ['302', '303'] and re.search(r'[' + latin_alphabet_regex_ + ']', t):
                    t = '<foreign>'
                    sentence += t + ' '
                    continue
                # if a number, <NUM>
                n = fix_nums(t, lang_code)
                if n.strip() != t.strip():
                    sentence += n.strip() + ' '
                    continue
                # 2. misc. fixes
                t = replace_ellipses(t)
                t = fix_hyphens(t, lang_code)
                t = fix_apostrophes(t, lang_code)
                # skip anything that's not allowed in our language
                # allow parens so we can remove what's inside them later
                if re.search(r'[^' + charset_regex_[lang_code] + "'" + r'\-\s\(\)\[\]@&\\\*~='+u'\u00b2'+r'\+#]', t): 
                    # also allowing '@' and remove the whole sentence later
                    # same with & \ * ~ = superscript-2 + #
                    # since we are not sure what these mean.
                    if len(t) > 1 and not re.search(r'[!\?\:]', t):
                        ## identify Lithuanian foreign words
                        if lang_code == '304':
                            # check that all characters are lat or ext-lat
                            for_regexp = r'^['+latin_alphabet_regex_+u'\u00c0-\u024f]+$'
                            if re.search(for_regexp, t.strip()):
                                sentence += '<foreign> '
                                continue
                        # else: bad data                 
                        print t
                    continue
                t = t.strip('-')
                t = fix_nums(t, lang_code)
                if t != '':
                    sentence += t + ' '
            # remove stage instructions etc. in ( ) [ ]
            sentence = re.sub(r'\(.*?\)', '', sentence)
            sentence = re.sub(r'\[.*\]', '', sentence)
            # remove sentences with '@' (emails) and '&' (like AT&T)
            if re.search(r'[@&\\\*~=\u00b2\+#]', sentence):
                continue
            # remove any stray punctuation
            sentence = re.sub('[\[\]\(\)\{\}]', '', sentence)
            # no empty sentences
            if sentence.strip() == '':
                continue
            # skip sentences that have URLs in them
            if re.search('http', sentence):
                continue
            if re.search('www', sentence):
                continue
            if re.search('WWW', sentence):
                continue
            sentence = re.sub('\s+', ' ', sentence)
            sentence = re.sub('(<NUM> )+', '<NUM> ', sentence)
            # disallow sentences that are all <foreign>
            sentence = '<s> ' + sentence.strip() + ' </s>\n'
            if re.search('<s> (<foreign> )+</s>', sentence):
                continue
            # 3. write out to file
            outf.write(sentence)
    outf.close()

def normalize_bbn():
    # for BBN data: do in parts.
    # because otherwise, training the tokenizer takes too long
    # and it doesn't gain much from the 16x additional data.
    # just keep re-using tokenizer from part '0'.
    for bbn in ['0', '1', '2', '3', '4', '5', '6', '7',
              '8', '9', 'a', 'b', 'c', 'd', 'e', 'f']:
        # 1. do prenormalization - put in a dictionary [genre -> text]
        prenorm_dict = {}
        genre = 'bbn'
        prenorm_dict.setdefault(genre, '')
        path = file_base_ + 'bbn/webtext_' + lc_
        for root, subdirs, files in os.walk(path, followlinks=True):
            if root.strip('/').split('/')[-1][0] != bbn:
                continue
            print '====' + root.strip('/').split('/')[-1] + '===='
            for f in files:
                filename = os.path.join(root, f)
                print filename
                normalized_text = pre_normalization(filename, lc_, genre)
                prenorm_dict[genre] += normalized_text + '\n'
        # 2. do sentence tokenization
        tokenizer = get_trained_tokenizer(lc_, prenorm_dict, bbn)
        for genre, prenorm_text in prenorm_dict.iteritems():
            outf = codecs.open('tmp_' + lc_ + '_' + genre + '_' + bbn + '.txt', 'w', encoding='utf-8')
            # 3. do post-normalization
            print 'STARTING POST-NORM: ' + genre
            sentences = tokenizer.sentences_from_text(prenorm_text)
            for s in sentences:
                postnorm_s = post_normalization(s, lc_)
                if postnorm_s == '':
                    continue
                else:
                    outf.write('<s> ' + postnorm_s + ' </s>\n')
            outf.close()
    # finally: merge partial files, and clean up.
    cmd = 'cat tmp_' + lc_ + '_bbn_* > ' + lc_ + '_bbn.txt'
    print cmd
    os.system(cmd)
    cmd = 'rm tmp_' + lc_ + '*'
    print cmd
    os.system(cmd)

    
def normalize_cu():
    # 1. Do prenormalization.
    prenorm_dict = {}
    for genre in genres_[lc_]:
        if genre == 'bbn' or genre == 'subtitles':
            continue
        else:
            prenorm_dict.setdefault(genre, '')
            path = file_base_ + 'columbia/' + lc_ + '/' + genre
            for root, subdirs, files in os.walk(path, followlinks=True):
                for f in files:
                    filename = os.path.join(root, f)
                    print filename
                    normalized_text = pre_normalization(filename, lc_, genre)
                    prenorm_dict[genre] += normalized_text + '\n'
    # 2. Do sentence tokenization.
    tokenizer = get_trained_tokenizer(lc_, prenorm_dict, '-1')
    for genre, prenorm_text in prenorm_dict.iteritems():
        outf = codecs.open(lc_ + '_' + genre + '.txt', 'w', encoding='utf-8')
        # 3. do post-normalization
        print 'STARTING POST-NORM: ' + genre
        sentences = tokenizer.sentences_from_text(prenorm_text)
        for s in sentences:
            postnorm_s = post_normalization(s, lc_)
            if postnorm_s == '':
                continue
            else:
                outf.write('<s> ' + postnorm_s + ' </s>\n')
        outf.close()

########## MAIN PROGRAM ##########

if __name__ == '__main__':
    # normalize_bbn()
    if 'subtitles' in genres_[lc_]:
        normalize_subtitles(lc_)
    if 'bbn' in genres_[lc_]:
        normalize_bbn()
    if len(genres_[lc_]) > 2:
        normalize_cu()
