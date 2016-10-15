#!/usr/bin/python
# coding=utf-8

__author__ = 'victor,erica,brian'

import codecs
import gzip
import os
from nltk.tokenize import punkt
import pymongo
from pymongo import MongoClient
import re
import time
import xml.etree.ElementTree as ET

# which language are we processing right now?
lc_ = '404'

# Describe your directory structure here.
# For each language we know about, which sources do we have and where
# does the data live?  The assumption is that all files with the
# expected extension (.xml or .xml.gz for subtitles; .txt or .txt.gz
# for everything else) in the directory tree for a source are to be
# processed.  It is also assumed that there are genre-specific
# files under the 'Lorelei' source directory, if it exists.
# One file per genre, as we were given with the data delivery.
# It is assumed that .json files have already been loaded into the MongoDB.

webroot = '/local2/ecooper/babel-textnorm/testdata/404_WEBDATA'

sources_ = { '202': { 'bbn'       : os.path.join(webroot, 'bbn/webtext_202'),
                      'subtitles' : os.path.join(webroot, '202-subtitles'),
                      },
             '404' : { 'bbn'      : os.path.join(webroot, 'BBN/BBN-Webtext/WEB_DATA_TXT'),
                       'bbn_filt' : os.path.join(webroot + '_filtered', 'BBN/BBN-Webtext/WEB_DATA_TXT_FILTERED'),
                       'ted'      : os.path.join(webroot, 'Lorelei/ted.txt'),
                       'wiki'     : os.path.join(webroot, 'Lorelei/wiki.txt'),
                 },
            }

# These languages do not use the Latin character set.
# Bengali, Pashto, Kazakh, Telugu, Amharic, Mongolian, Georgian.
non_latin_ = ['103', '104', '302', '303', '307', '401', '404']

# for MongoDB:
# map numeric language code to string identifier
mongo_lc_ = {
    '103' : 'ben',
    '104' : 'pus',
    '105' : 'tur',
    '201' : 'ht',
    '206' : 'zul',
    '303' : 'tgl',
    '305' : 'gug',
    '306' : 'ig',
    '307' : 'amh',
    '401' : 'mon',
    '402' : 'jav',
    '403' : 'luo',
    '404' : 'kat',
    }

mongo_genres_ = ['tweets', 'blogPosts', 'forumPosts']

tokenizer_ = None

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

# 103 (Bengali) uses Unicode range U+0980-U+09FF.  In some cases where separate
# rendering must be forced, such as for morphological boundaries or loan words,
# zero-width characters (U+200c and U+200d) may be used.

# 104 (Pashto) is written in an extended Arabic script. The script consists of
# 27 standard Arabic script letters (plus Alef Madda), plus 17 symbols from an
# extended Arabic set. The Zero-Width-Non-Joiner (ZWNJ) character is a
# non-printing character that is included in the Pashto database. The Unicode
# is U+200C. This character is placed between two characters that would
# otherwise be connected into a ligature, to ensure that they are displayed in
# their final and initial forms, respectively.

# 106 (Tagalog) uses the Latin character set plus n-tilde (U+00D1 (uppercase)
# and U+00F1 (lowercase)).

# 201 (Haitian Creole) uses the basic Latin alphabet plus \u00e0, \u00e8, and
# \u00f2.

# 202 (Swahili) uses the basic Latin alphabet.

# 205 (Kurmanji) uses the basic Latin characters 41-7A
#  (notice that this interval contains [ \ ] ^ _ ` , those have been removed)
# plus 10 additional characters from LATIN_EXTENDED_A

# 206 (Zulu) uses the basic Latin character set.

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

# 305 (Paraguayan Guarani) uses the Latin alphabet plus some additional forms
# from the Latin-1 supplement set, Latin Extended-A, and the Latin Extended
# Additional set.  The following are additional forms for Paraguayan Guarani:
# 00c1 00c3 00c9 00cd 00d1 00d3 00d5 00da 00dd 00e1 00e3 00e9 00ed 00f1 00f3
# 00f5 00fa 00fc 00fd 0128 0129 0168 0169 1ebc 1ebd 1ef8 1ef9

# 306 (Igbo) uses the Latin alphabet plus some additional diacritized Latin
# characters (1ECA-1ECD, 1E44-1E45, 1EE4-1EE5), but the LSP says these are
# frequently misused so they don't use them at all in the transcripts.
# Transcripts have been checked and this is verified; no diacritics, so we'll
# replace those.

# 307 (Amharic) uses the Ge'ez script, fidel.  The Unicode range for Ethiopic
# scripts is U+1200-U+137f.

# 401 (Mongolian) - Today, the classic Mongolian script is still used in Inner
# Mongolia, but the official standard spelling of Halh Mongolian uses Mongolian
# Cyrillic. This is also the script used for all educational purposes in
# Mongolia, and therefore the script which was used for this project. It
# consists of the standard Cyrillic range (Ux0410-Ux044F, Ux0401, and Ux0451)
# plus two extra characters, Ux04E8/Ux04E9 and Ux04AE/Ux04AF.

# 402 (Javanese) uses the Latin script.  Javanese can also be written in the
# traditional Javanese script Hanacaraka, however it is uncommon.  There is a
# third script, Pegon, which is a form of Arabic adapted to accomodate Javanese
# speech sounds, however this is restricted to religious manuscripts.

# 403 (Dholuo) uses the Latin script.

# 404 (Georgian) uses its own alphabet.
# The script used to write modern standard Georgian is Mkhedruli.
# This alphabet is found in the Unicode range 10D0-10F0.
# Latin numerals are used.

numeral_regex_ = {'103': latin_numeral_regex_ + u'\u09e6-\u09ef',
                  '104': latin_numeral_regex_ + u'\u0660-\u0669\u06f0-\u06f9',
                  '106': latin_numeral_regex_,
                  '201': latin_numeral_regex_,
                  '202': latin_numeral_regex_,
                  '205': latin_numeral_regex_,
                  '206': latin_numeral_regex_,
                  '207': latin_numeral_regex_,
                  '301': latin_numeral_regex_,
                  '302': latin_numeral_regex_,
                  '303': latin_numeral_regex_ + u'\u0c66-\u0c6f\u0c78-\u0c7f',
                  '304': latin_numeral_regex_,
                  '305': latin_numeral_regex_,
                  '306': latin_numeral_regex_,
                  '307': latin_numeral_regex_ + u'\u1369-\u137c',
                  '401': latin_numeral_regex_,
                  '402': latin_numeral_regex_,
                  '403': latin_numeral_regex_,
                  '404': latin_numeral_regex_,
                  }

# from the LSP
table_of_numbers_ = {
    '103' : {
        '0' : u'\u09b6\u09c2\u09a8\u09cd\u09af',
        u'\u09e6' : u'\u09b6\u09c2\u09a8\u09cd\u09af',
        '1' : u'\u098f\u0995',
        u'\u09e7' : u'\u098f\u0995',
        '2' : u'\u09a6\u09c1\u0987',
        u'\u09e8' : u'\u09a6\u09c1\u0987',
        '3' : u'\u09a4\u09bf\u09a8',
        u'\u09e9' : u'\u09a4\u09bf\u09a8',
        '4' : u'\u099a\u09be\u09b0',
        u'\u09ea' : u'\u099a\u09be\u09b0',
        '5' : u'\u09aa\u09be\u0981\u099a',
        u'\u09eb' : u'\u09aa\u09be\u0981\u099a',
        '6' : u'\u099b\u09af\u09bc',
        u'\u09ec' : u'\u099b\u09af\u09bc',
        '7' : u'\u09b8\u09be\u09a4',
        u'\u09ed' : u'\u09b8\u09be\u09a4',
        '8' : u'\u0986\u099f',
        u'\u09ee' : u'\u0986\u099f',
        '9' : u'\u09a8\u09af\u09bc',
        u'\u09ef' : u'\u09a8\u09af\u09bc',
        # '10' : ## didn't render in the PDF
        # u'\u09e7\u09e6' : ## didn't render in the PDF
        # '100' : ## LSP has two options
        # u'\u09e7\u09e6\u09e6' : ## LSP has two options
        '10000' : u'\u09a6\u09b6\u09b9\u09be\u099c\u09be\u09b0',
        u'\u09e7\u09e6\u09e6\u09e6\u09e6' : u'\u09a6\u09b6\u09b9\u09be\u099c\u09be\u09b0',
        # '100000' : ## LSP has two options
        # u'\u09e7\u09e6\u09e6\u09e6\u09e6\u09e6' : ## LSP has two options
        # '10000000' : ## LSP has two options
        # u'\u09e7\u09e6\u09e6\u09e6\u09e6\u09e6\u09e6\u09e6' :
        },
    '104' : {
        '0' : u'\u0635\u0641\u0631',
        u'\u0660' : u'\u0635\u0641\u0631',
        u'\u06f0' : u'\u0635\u0641\u0631',
        '1' : u'\u064a\u0648',
        u'\u0661' : u'\u064a\u0648',
        u'\u06f1' : u'\u064a\u0648',
        '2' : u'\u062f\u0648\u0647',
        u'\u0662' : u'\u062f\u0648\u0647',
        u'\u06f2' : u'\u062f\u0648\u0647',
        '3' : u'\u062f\u0631\u06d0',
        u'\u0663' : u'\u062f\u0631\u06d0',
        u'\u06f3' : u'\u062f\u0631\u06d0',
        '4' : u'\u0685\u0644\u0648\u0631',
        u'\u0664' : u'\u0685\u0644\u0648\u0631',
        u'\u06f4' : u'\u0685\u0644\u0648\u0631',
        '5' : u'\u067e\u067b\u0646\u0681\u0647',
        u'\u0665' : u'\u067e\u067b\u0646\u0681\u0647',
        u'\u06f5' : u'\u067e\u067b\u0646\u0681\u0647',
        '6' : u'\u0634\u067e\u0696',
        u'\u0666' : u'\u0634\u067e\u0696',
        u'\u06f6' : u'\u0634\u067e\u0696',
        '7' : u'\u0627\u0648\u0648\u0647',
        u'\u0667' : u'\u0627\u0648\u0648\u0647',
        u'\u06f7' : u'\u0627\u0648\u0648\u0647',
        '8' : u'\u0627\u062a\u0647',
        u'\u0668' : u'\u0627\u062a\u0647',
        u'\u06f8' : u'\u0627\u062a\u0647',
        '9' : u'\u0646\u06be\u0647',
        u'\u0669' : u'\u0646\u06be\u0647',
        u'\u06f9' : u'\u0646\u06be\u0647',
        '10' : u'\u0644\u0633',
        '11' : u'\u064a\u0648\u0648\u0644\u0633',
        '12' : u'\u062f\u0648\u0648\u0644\u0633',
        '13' : u'\u062f\u064a\u0627\u0631\u0644\u0633',
        '14' : u'\u0685\u0648\u0627\u0631\u0644\u0633',
        '15' : u'\u067e\u067b\u0646\u0681\u0644\u0633',
        '16' : u'\u0634\u067e\u0627\u0693\u0633',
        '17' : u'\u0627\u0648\u0648\u0644\u0633',
        '18' : u'\u0627\u062a\u0644\u0633',
        '19' : u'\u0646\u0648\u0644\u0633',
        '20' : u'\u0634\u0644',
        '30' : u'\u062f\u067b\u0631\u0634',
        '40' : u'\u0685\u0644\u0648\u067b\u069a\u062a',
        '50' : u'\u067e\u067b\u0646\u0681\u0648\u0633',
        '60' : u'\u0634\u067e\u067b\u062a\u0647',
        '70' : u'\u0627\u0648\u064a\u0627',
        '80' : u'\u0627\u062a\u064a\u0627',
        '90' : u'\u0646\u0648\u064a',
        '100' : u'\u0633\u0644',
        '1000' : u'\u0632\u0631',
        '100000' : u'\u064a\u0648\u0644\u06a9',
        '1000000' : u'\u0645\u064a\u0644\u064a\u0648\u0646',
        '10000000' : u'\u0644\u0633 \u0645\u064a\u0644\u064a\u0648\u0646\u0647',
        },
    '106' : {
        # Numbers can be Tagalog, English, or Spanish.
        },
    '201' : {
        '0' : u'zewo',
        '1' : u'en',
        '2' : u'de',
        '3' : u'twa',
        '4' : u'kat',
        '5' : u'senk',
        '6' : u'sis',
        '7' : u's\u00e8t',
        '8' : u'uit',
        '9' : u'n\u00e8f',
        '10' : u'dis',
        '11' : u'onz',
        '12' : u'douz',
        '13' : u'tr\u00e8z',
        '14' : u'kat\u00f2z',
        '15' : u'kenz',
        '16' : u's\u00e8z',
        '17' : u'dis\u00e8t',
        '18' : u'dizuit',
        '19' : u'dizn\u00e8f',
        '20' : u'ven',
        '21' : u'venteyen',
        '22' : u'vennde',
        '23' : u'venntwa',
        '24' : u'vennkat',
        '25' : u'vennsenk',
        '26' : u'vennsis',
        '27' : u'venns\u00e8t',
        '28' : u'ventuit',
        '29' : u'ventn\u00e8f',
        '30' : u'trant',
        '31' : u'tranteyen',
        '32' : u'trannde',
        '33' : u'tranntwa',
        '34' : u'trannkat',
        '35' : u'trannsenk',
        '36' : u'trannsis',
        '37' : u'tranns\u00e8t',
        '38' : u'trantuit',
        '39' : u'trantn\u00e8f',
        '40' : u'karant',
        '41' : u'karanteyen',
        '42' : u'karannde',
        '43' : u'karanntwa',
        '44' : u'karannkat',
        '45' : u'karannsenk',
        '46' : u'karannsis',
        '47' : u'karanns\u00e8t',
        '48' : u'karant uit',
        '49' : u'karant n\u00e8f',
        '50' : u'senkant',
        '51' : u'senkanteyen',
        '52' : u'senkannde',
        '53' : u'senkanntwa',
        '54' : u'senkannkat',
        '55' : u'seknakksenk',
        '56' : u'seknannsis',
        '57' : u'senkanns\u00e8t',
        '58' : u'senkantuit',
        '59' : u'senkantn\u00e8f',
        '60' : u'swasant',
        '61' : u'swasanteyen',
        '62' : u'swasannde',
        '63' : u'swasanntwa',
        '64' : u'swasannkat',
        '65' : u'swasannsenk',
        '66' : u'swasannsis',
        '67' : u'swasantuit',
        '68' : u'swasantn\u00e8f',
        '69' : u'swasant n\u00e8f',
        '70' : u'swasanndis',
        '71' : u'swasannnonz',
        '72' : u'swasanndouz',
        '73' : u'swasanntr\u00e8z',
        '74' : u'swasannkat\u00f2z',
        '75' : u'swasannkenz',
        '76' : u'swasanns\u00e8z',
        '77' : u'swasanndis\u00e8t',
        '78' : u'swasanndizuit',
        '79' : u'swasanndizn\u00e8f',
        '80' : u'katreven',
        '81' : u'katrevenen',
        '82' : u'katrevende',
        '83' : u'katreventwa',
        '84' : u'katrevenkat',
        '85' : u'katrevensenk',
        '86' : u'katrevensis',
        '87' : u'katrevens\u00e8t',
        '88' : u'katrevenuit',
        '89' : u'katrevenn\u00e8f',
        '90' : u'katrevendis',
        '91' : u'katrevenonz',
        '92' : u'katrevendouz',
        '93' : u'katreventr\u00e8z',
        '94' : u'katrevenkat\u00f2z',
        '95' : u'katrevenkenz',
        '96' : u'katrevens\u00e8z',
        '97' : u'katrevendis\u00e8t',
        '98' : u'katrevendizuit',
        '99' : u'katrevendizn\u00e8f',
        '100' : u'san',
        '10000' : u'di mil',
        '100000' : u'san mil',
        '1000000' : u'en milyon',
        '10000000' : u'di milyon',
        '1000000000' : u'en milya',
        },
    '202' : {
        # 0 -- has two variants
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
        #'40' -- has two variants
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
    '206' : {
        # the numbers take on different forms depending on what they are
        # counting, so we can't reliably replace them without knowing more.
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
        #'0' : u'', has two variants
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
        },
    '304' : {
        # Lithuanian numbers have case which means that the word might change
        # depending on what it's describing.  Cannot confidently replace them
        # without knowing more so leaving this empty.
        },
    '305' : { # using the nativized Spanish versions where possible because
              # the LSP says that's what people use more in conversation
        '0' : u's\u00e9ro',
        '1' : u'pete\u0129',
        '2' : u'mok\u00f5i',
        '3' : u'mbohapy',
        '4' : u'ku\u00e1tro',
        '5' : u's\u00ednko',
        '6' : u's\u00e9ih',
        '7' : u'si\u00e9te',
        '8' : u'\u00f3cho',
        '9' : u'nu\u00e9ve',
        '10' : u'dieh',
        '100' : u'si\u00e9nto',
        '1000' : u'mil',
        '10000' : u'dieh mil',
        '100000' : u'si\u1ebd mil',
        '1000000' : u'\u0169 mill\u00f5',
        '10000000' : u'dieh mill\u00f3neh',
        },
    '306' : {
        # '0' : u'', # LSP gives two variants
        '1' : u'out',
        '2' : u'abuo',
        '3' : u'ato',
        '4' : u'ano',
        '5' : u'ise',
        '6' : u'isii',
        '7' : u'asaa',
        '8' : u'asato',
        # '9' : u'', # LSP gives two variants
        '10' : u'iri',
        '100' : u'nari',
        '1000' : u'puku',
        '10000' : u'puku iri',
        '100000' : u'puku nari',
        '1000000' : u'nde',
        '10000000' : u'nde iri',
        '1000000000' : u'ijeri',
        },
    '307' : {
        # '0' : LSP has 3 variants
        '1' : u'\u12a0\u1295\u12f5',
        u'\u1369' : u'\u12a0\u1295\u12f5',
        '2' : u'\u1201\u1208\u1275',
        u'\u136a' : u'\u1201\u1208\u1275',
        '3' : u'\u1226\u1235\u1275',
        u'\u136b' : u'\u1226\u1235\u1275',
        '4' : u'\u12a0\u122b\u1275',
        u'\u136c' : u'\u12a0\u122b\u1275',
        '5' : u'\u12a0\u121d\u1235\u1275',
        u'\u136d' : u'\u12a0\u121d\u1235\u1275',
        '6' : u'\u1235\u12f5\u1235\u1275',
        u'\u136e' : u'\u1235\u12f5\u1235\u1275',
        '7' : u'\u1230\u1263\u1275',
        u'\u136f' : u'\u1230\u1263\u1275',
        '8' : u'\u1235\u121d\u1295\u1275',
        u'\u1370' : u'\u1235\u121d\u1295\u1275',
        '9' : u'\u12d8\u1320\u129d',
        u'\u1371' : u'\u12d8\u1320\u129d',
        '10' : u'\u12d0\u1235\u122d',
        u'\u1372' : u'\u12d0\u1235\u122d',
        # '100' : ## LSP has 2 variants
        # u'\u137b' : ## LSP has 2 variants
        # '1000' : ## LSP has 2 variants 
        # u'\u1372\u137b' : ## LSP has 2 variants
        '10000' : u'\u12d0\u1235\u122d \u123a\u1205',
        u'\u137c' : u'\u12d0\u1235\u122d \u123a\u1205',
        '100000' : u'\u1218\u1276 \u123a\u1205',
        u'\u1372\u137b\u137b' : u'\u1218\u1276 \u123a\u1205',
        '1000000' : u'\u1218\u1276 \u123a\u1205',
        '10000000' : u'\u12a0\u1235\u122d \u121a\u120a\u12ee\u1295',
        },
    '401' : {
        ## there are combining forms which we won't know how to replace.
        },
    '402' : {
        ## most have 2 variants.  Just including ones with 1 variant.
        '5' : u'lima',
        '6' : u'enem',
        '8' : u'wolu',
        '9' : u'sanga',
        },
    '403' : {
        '0' : u'nono',
        '1' : u'achiel',
        '2' : u'ariyo',
        '3' : u'adek',
        '4' : u"ang'wen",
        '5' : u'abich',
        '6' : u'auchiel',
        '7' : u'abiriyo',
        '8' : u'aboro',
        '9' : u'ochiko',
        '10' : u'apar',
        # everything else has 2 variants
        },
    '404' : {
        '0' : u'\u10dc\u10e3\u10da\u10d8',
        '1' : u'\u10d4\u10e0\u10d7\u10d8',
        '2' : u'\u10dd\u10e0\u10d8',
        '3' : u'\u10e1\u10d0\u10db\u10d8',
        '4' : u'\u10dd\u10d7\u10ee\u10d8',
        '5' : u'\u10ee\u10e3\u10d7\u10d8',
        '6' : u'\u10d4\u10e5\u10d5\u10e1\u10d8',
        '7' : u'\u10e8\u10d5\u10d8\u10d3\u10d8',
        '8' : u'\u10e0\u10d5\u10d0',
        '9' : u'\u10ea\u10ee\u10e0\u10d0',
        '10' : u'\u10d0\u10d7\u10d8',
        '100' : u'\u10d0\u10e1\u10d8',
        '10000' : u'\u10d0\u10d7\u10d8 \u10d0\u10d7\u10d0\u10e1\u10d8',
        '100000' : u'\u10d0\u10e1\u10d8 \u10d0\u10d7\u10d0\u10e1\u10d8',
        '10000000' : u'\u10db\u10d8\u10da\u10d8\u10dd\u10dc\u10d8',
        }
    }

# As defined by the language packs.
# The alphabet of the language.
# These do not include punctuation or numerals.
charset_regex_ = {'103' : u'\u0980-\u09ff\u200c\u200d',
                  '104' : u'\u0621\u0622\u0626\u0627\u0628\u062a\u062b\u062c\u062d\u062e\u062f\u0630\u0631\u0632\u0633\u0634\u0635\u0636\u0637\u0638\u0639\u063a\u0641\u0642\u0644\u0645\u0646\u0647\u0648\u064a\u064b\u067c\u067e\u0681\u0685\u0686\u0689\u0693\u0696\u0698\u069a\u06a9\u06ab\u06bc\u06cc\u06cd\u06d0\u200c',
                  '106' : latin_alphabet_regex_ + u'\u00d1\u00f1',
                  '201' : latin_alphabet_regex_ + u'\u00e0\u00e8\u00f2',
                  '202' : latin_alphabet_regex_ + u'\u00e9\u00e1',
                  '205': u'\u0041-\u005a\u0061-\u007a\u00c7\u00ca\u00ce\u00db\u015e\u00e7\u00ea\u00ee\u00fb\u015f',
                  '206': latin_alphabet_regex_,
                  '207': latin_alphabet_regex_,
                  '301': u'\u0041-\u005a\u0061-\u007a\u00d1\u00f1',
                  '302': u'\u0400-\u04ff\u0451\u0456\u0492\u0493\u049a\u049b\u04a2\u04a3\u04ae\u04af\u04b0\u04b1\u04ba\u04bb\u04d8\u04d9\u04e8\u04e9\u0401\u0406',
                  '303': u'\u0c00-\u0c7f\u200c',
                  '304': u'\u0041-\u005a\u0061-\u007a\u0104\u0105\u010c\u010d\u0116\u0117\u0118\u0119\u012e\u012f\u0160\u0161\u016a\u016b\u0172\u0173\u017d\u017e',
                  '305': latin_alphabet_regex_ + u'\u00c1\u00c3\u00c9\u00cd\u00d1\u00d3\u00d5\u00da\u00dd\u00e1\u00e3\u00e9\u00ed\u00f1\u00f3\u00f5\u00fa\u00fc\u00fd\u0128\u0129\u0168\u0169\u1ebc\u1ebd\u1ef8\u1ef9',
                  '306': latin_alphabet_regex_ + u'\u1eca-\u1ecd\u1e44-\u1e45\u1ee4-\u1ee5',
                  '307': u'\u1200-\u137f',
                  '401': u'\u0410-\u044f\u0401\u0451\u04e8\u04e9\u04ae\u04af',
                  '402': latin_alphabet_regex_,
                  '403': latin_alphabet_regex_,
                  '404': u'\u10d0-\u10f0',
                  }

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

def fix_diacritics(text, lang_code):
    # only Igbo for now
    if lang_code != '306':
        return text
    else:
        res = re.sub(u'\u1ecb', u'i', text)
        res = re.sub(u'\u1eca', u'I', res)
        res = re.sub(u'\u1e45', u'n', res)
        res = re.sub(u'\u1e44', u'N', res)
        res = re.sub(u'\u1ecd', u'o', res)
        res = re.sub(u'\u1ecc', u'O', res)
        res = re.sub(u'\u1ee5', u'u', res)
        res = re.sub(u'\u1ee4', u'U', res)

        # alternate forms not in the LSP:
        # letter combined with dot
        res = re.sub(u'i\u0323', u'i', res)
        res = re.sub(u'I\u0323', u'I', res)
        res = re.sub(u'o\u0323', u'o', res)
        res = re.sub(u'O\u0323', u'O', res)
        res = re.sub(u'u\u0323', u'u', res)
        res = re.sub(u'U\u0323', u'U', res)
        res = re.sub(u'n\u0307', u'n', res)
        res = re.sub(u'N\u0307', u'N', res)

        return res


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
def prenorm(unnormalized_text, lang_code, genre):
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

## Helper function for normalizing text that is in a file
def prenorm_file(filename, lang_code, genre):
    if filename.endswith('.gz'):
        f = codecs.getreader('utf-8')(gzip.open(filename, 'rb'))
    else:
        f = codecs.open(filename, 'r', encoding='utf-8')
    unnormalized_text = f.read().strip()
    return prenorm(unnormalized_text, lang_code, genre)

# Return a trained tokenizer.
# Train a new one if you don't have one already.
# For bbn web data, just train on the first slice of the data.
def get_trained_tokenizer(lang_code, prenorm_text, seg):
    global tokenizer_
    if (seg != '0' and seg != '-1'):
        print 'USING TRAINED TOKENIZER'
        print 'SEG VALUE: ' + str(seg)
        return tokenizer_
    else:
        print 'TRAINING NEW TOKENIZER'
        global punkt
        punkt = reload(punkt)
        tokenizer = punkt.PunktSentenceTokenizer()
        params = tokenizer.train(prenorm_text)
        if seg == '0':
            print 'SETTING BBN TOKENIZER'
            tokenizer_ = tokenizer
        else:
            print 'SETTING CU TOKENIZER'
            tokenizer_ = tokenizer
        print 'DONE TRAINING TOKENIZER'
        return tokenizer

## Mongolian web users often make some substitutions when they are using
## a Russian keyboard.  See here:
## https://en.wikipedia.org/wiki/Mongolian_Cyrillic_alphabet
## In particular, we see a lot of 'v' in our web data because of this.
## We choose to normalize to the correct Mongolian characters
## so that we can preserve more of the data and have it match what's in the LP.
def fix_mongolian(s):
    res = re.sub(u'\u0407', u'\u04ae', s)
    res = re.sub(u'\u0457', u'\u04af', res)
    res = re.sub(u'V', u'\u04ae', res)
    res = re.sub(u'v', u'\u04af', res)
    res = re.sub(u'\u0404', u'\u04e8', res)
    res = re.sub(u'\u0454', u'\u04e9', res)
    return res

## we see some gt; and lt; in the data that is causing the Latin detector
## to reject sentences that are otherwise ok.  Remove these.
## also remove from latin script languages because they should be removed.
## also amp; but it keeps e.g. clamp; stamp; etc.
def fix_lt_gt(s):
    res = re.sub('( |^)((amp;)|(l|g)t;)+', ' ', s)
    return res

def post_normalization(s, lang_code):
    # skip any sentences that contain Latin alphabet.
    # this is presumed to include URLs.
    s = fix_lt_gt(s)
    if lang_code in non_latin_:
        if lang_code == '401':
            s = fix_mongolian(s)
        match_obj = re.search(r'[' + latin_alphabet_regex_ + ']', s)
        if match_obj:
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
    # for Igbo: replace diacritized characters with non-diacritized
    res = fix_diacritics(res, lang_code)
    # done
    return res.strip()

## Helper function: post-normalize and write to file.
def write_postnorm(outf, tokenizer, prenorm_text):
    print 'STARTING POST-NORMALIZATION'
    sentences = tokenizer.sentences_from_text(prenorm_text)
    for s in sentences:
        postnorm_s = post_normalization(s, lc_)
        if postnorm_s != '':
            outf.write('<s> ' + postnorm_s + ' </s>\n')    

### Normalizer specifically for subtitles.
### Just need to parse out the XML and clean up any out-of-language text.
### It's already in utterances, so no need to tokenize.
### Punctuation is already separated out, mostly.
def normalize_subtitles(path):
    outf = codecs.open(lc_ + '_subtitles.txt', 'w', encoding='utf-8')
    # 1. Parse out the sentences
    for root, subdirs, files in os.walk(path, followlinks=True):
        for f in files:
            filename = os.path.join(root, f)
            print filename
            if filename.endswith('.gz'):
                tree = ET.parse(gzip.open(filename, 'rb'))
            else:
                tree = ET.parse(filename)
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
                    if lc_ in ['302', '303'] and re.search(r'[' + latin_alphabet_regex_ + ']', t):
                        t = '<foreign>'
                        sentence += t + ' '
                        continue
                    # if a number, <NUM>
                    n = fix_nums(t, lc_)
                    if n.strip() != t.strip():
                        sentence += n.strip() + ' '
                        continue
                    # 2. misc. fixes
                    t = replace_ellipses(t)
                    t = fix_hyphens(t, lc_)
                    t = fix_apostrophes(t, lc_)
                    # skip anything that's not allowed in our language
                    # allow parens so we can remove what's inside them later
                    if re.search(r'[^' + charset_regex_[lc_] + "'" + r'\-\s\(\)\[\]@&\\\*~='+u'\u00b2'+r'\+#]', t): 
                        # also allowing '@' and remove the whole sentence later
                        # same with & \ * ~ = superscript-2 + #
                        # since we are not sure what these mean.
                        if len(t) > 1 and not re.search(r'[!\?\:]', t):
                            ## identify Lithuanian foreign words
                            if lc_ == '304':
                                # check that all characters are lat or ext-lat
                                for_regexp = r'^['+latin_alphabet_regex_+u'\u00c0-\u024f]+$'
                                if re.search(for_regexp, t.strip()):
                                    sentence += '<foreign> '
                                    continue
                            # else: bad data                 
                            print t
                        continue
                    t = t.strip('-')
                    t = fix_nums(t, lc_)
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
    path = sources_[lc_]['bbn']
    # for BBN data: do in parts.
    # because otherwise, training the tokenizer takes too long
    # and it doesn't gain much from the 16x additional data.
    # just keep re-using tokenizer from part '0'.
    print "PATH: " + path
    for seg in ['0', '1', '2', '3', '4', '5', '6', '7',
              '8', '9']:
        # 1. do prenormalization - put in a dictionary [genre -> text]
        prenorm_text = ''
        genre = 'bbn'
        print 'PATH: ' + path
        for root, subdirs, files in os.walk(path, followlinks=True):
            if root.strip('/').split('/')[-1][-1:] != seg:
                # groups by last digit
                continue
            print '====' + root.strip('/').split('/')[-1] + '===='
            for f in files:
                filename = os.path.join(root, f)
                print filename
                prenorm_text += prenorm_file(filename, lc_, genre) + '\n'
        # 2. do sentence tokenization
        tokenizer = get_trained_tokenizer(lc_, prenorm_text, seg)
        # 3. postnorm
        outf = codecs.open('tmp_' + lc_ + '_' + genre + '_' + seg + '.txt',
                           mode='w', encoding='utf-8')
        write_postnorm(outf, tokenizer, prenorm_text)
        outf.close()
        
    # finally: merge partial files, and clean up.
    cmd = 'cat tmp_' + lc_ + '_bbn_* > ' + lc_ + '_bbn.txt'
    print cmd
    os.system(cmd)
    cmd = 'rm tmp_' + lc_ + '*'
    print cmd
    os.system(cmd)

def normalize_bbn_filtered():
    path = sources_[lc_]['bbn_filt']
    print 'PATH: ' + path
    outf = codecs.open(lc_ + '_bbn_filtered.txt', mode='w', encoding='utf-8')
    for root, subdirs, files in os.walk(path, followlinks=True):
        count = -1
        for f in files:
            print 'FILE: ' + f
            prenorm_text = ''
            count += 1
            filename = os.path.join(root, f)
            print filename
            prenorm_text += prenorm_file(filename, lc_, 'bbn') + '\n'
            ## just train on the first one
            tokenizer = get_trained_tokenizer(lc_, prenorm_text, str(count))
            write_postnorm(outf, tokenizer, prenorm_text)
    outf.close()
                                                
def normalize_ted():
    # 1. Do prenormalization.
    prenorm_text = ''
    path = sources_[lc_]['ted']
    prenorm_text += prenorm_file(path, lc_, 'TED') + '\n'
    # 2. Do sentence tokenization.
    tokenizer = get_trained_tokenizer(lc_, prenorm_text, '-1')
    # 3. do post-normalization and write output
    outf = codecs.open(lc_ + '_ted.txt', mode='w', encoding='utf-8')
    write_postnorm(outf, tokenizer, prenorm_text)
    outf.close()

def normalize_mongo():
    db_time_1 = 0
    db_time_2 = 0
    prenorm_time_ = 0
    # Normalize 20k documents at a time.
    # Otherwise, it uses too much memory.
    # Just train the sentence tokenizer on the first 20k documents, and reuse it.
    
    # 0. Read from the DB
    client = MongoClient()
    db = client['scraping']

    # 1.  Do prenormalization.
    for genre in mongo_genres_:
        prenorm_text = ''
        # ^ reset for each genre so we don't keep around genres we're done with.
        genre_posts = db[genre]

        results = genre_posts.find({'languageCode': mongo_lc_[lc_]})
        howmany = results.count()

        print genre + ': ' + str(howmany) + ' documents'
        batch_size = 20000
        last_batch_size = howmany % batch_size
        print 'Batches: ' + str((howmany / batch_size) + 1)
        outf = codecs.open(lc_ + '_' + genre + '.txt', 'w', encoding='utf-8')

        for i in range(0,(howmany/batch_size)+1):
            # last batch? avoid index errors.
            if i == (howmany/batch_size):
                this_batch_size = last_batch_size
            else:
                this_batch_size = batch_size

            prenorm_text = ''
            print str(i)
            results = list(genre_posts.find({'languageCode':mongo_lc_[lc_]})[batch_size*i:batch_size*i+this_batch_size]) # this is a cursor object until we cast it as list
            
            print 'PRE-NORMALIZING'
            
            for j in range(0, this_batch_size):
                a = time.time()
                post = results[j] 
                b = time.time()
                db_time_1 += b - a
                c = time.time()
                p = post['data']
                d = time.time()
                db_time_2 += d - c
                e = time.time()
                prenorm_post = prenorm(p.strip(), lc_, 'mongo')
                f = time.time()
                prenorm_time_ += f - e
                prenorm_text += prenorm_post + '\n'
        
            # 2. Sentence tokenization (for this batch)
            if i == 0: # just train tokenizer on first batch
                tokenizer = get_trained_tokenizer(lc_, prenorm_text, '-1')
            else: ## get trained tokenizer
                tokenizer = get_trained_tokenizer(lc_, prenorm_text, '1')

            # 3. Do post-normalization for this batch
            print 'POST-NORMALIZING: ' + genre
            write_postnorm(outf, tokenizer, prenorm_text)

        print 'DONE WITH GENRE: ' + genre
        outf.close()
        print 'Time spent on DB (1): ' + str(db_time_1)
        print 'Time spent on DB (2): ' + str(db_time_2)
        print 'Time spent in prenorm: ' + str(prenorm_time_)

## All Georgian wiki data contains some Latin text, so we have to allow <foreign>.
## It is also already sentence-segmented in the form it was provided.
def normalize_wiki():
    genre = 'wiki'
    filename = sources_[lc_]['wiki']
    print filename
    outf = codecs.open(lc_ + '_wiki.txt', mode='w', encoding='utf-8')
    f = codecs.open(filename, 'r', encoding='utf-8')
    for line in f:
        if line == '':
            continue
        res = prenorm(line, lc_, genre)

        # not using regular postnorm because we have to allow <foreign>.
        s = fix_lt_gt(res)
        res = remove_urls(s)
        res = res.strip('.')
        res = fix_abbrevs(res)        
        res = re.sub(',', '', res)
        keep_chars = u'\u005f' + numeral_regex_[lc_] + charset_regex_[lc_] + latin_alphabet_regex_ + '\-' + "'"  
        res = re.sub(r'[^' + keep_chars + r']', ' ', res) 

        if lc_ in non_latin_:
            # replace any continuous string of latin alphabet chars with <foreign>
            latinword = r'[' + latin_alphabet_regex_ + ']+'
            res = re.sub(latinword, ' <foreign> ', res)
        res = re.sub('\s+', ' ', res)
        # remove duplicate <foreign>s
        res = re.sub('(<foreign> )+', '<foreign> ', res)
        res = fix_nums(res, lc_)
        # collapse multiple <NUM>s
        res = re.sub('(<NUM> )+', '<NUM> ', res)
        res = re.sub(r'\s+', ' ', res)
        res = re.sub(r'_+', '_', res)
        res = fix_diacritics(res, lc_)
        # if it's only <foreign> and <num>, we should skip it
        haslangchars = re.search('[' + charset_regex_[lc_] + ']', res)
        if not haslangchars:
            res = ''
        if res != '':
            outf.write('<s> ' + res.strip() + ' </s>\n')
    outf.close()
    
########## MAIN PROGRAM ##########

if __name__ == '__main__':
    #normalize_subtitles()
    normalize_bbn()
    normalize_bbn_filtered()
    normalize_ted()
    normalize_mongo()
    normalize_wiki()

    
