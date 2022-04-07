#!python
#distutils: language=3
#distutils: language = c++

#Above headers are for Cython compiler

import numpy
import cython
from spacy.tokens.doc cimport Doc
from spacy.structs cimport TokenC

# TODO: check for stop words and return array of lemmatized word tokens instead lemma text string
def lemma_txt(Doc doc):
    lemma_token_list = []
    cdef TokenC * token_ptr
    for token_ptr in doc.c[:doc.length]:
        lemma_token_list.append(doc.vocab.strings[token_ptr.lemma])
    token_str = ' '.join(lemma_token_list)
    return token_str
