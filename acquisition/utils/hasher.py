"""Utility for generating the raw hashes."""

import itertools
import string

# Dictionary for characters from range 1 - 26.
h_26 = {k: v for k, v in enumerate(string.ascii_lowercase, start=1)}
# Dictionary for characters from range 1 - 676.
t_676 = itertools.product(string.ascii_lowercase, string.ascii_lowercase)
h_676 = {k: v for k, v in enumerate(list(map(''.join, t_676)), start=1)}
# Dictionary for characters from range 1 - 17576.
t_17k = itertools.product(string.ascii_lowercase,
                          string.ascii_lowercase,
                          string.ascii_lowercase)
h_17k = {k: v for k, v in enumerate(list(map(''.join, t_17k)), start=1)}
# Dictionary for hashing area.
h_area = {'p': 'Parking lot',
          'g': 'Garage',
          'c': 'Counter',
          'k': 'Kitchen',
          'l': 'Lobby',
          'a': 'Aisle',
          'b': 'Basement',
          'f': 'Cafeteria',
          'o': 'Outdoor area',
          'w': 'Workdesk',
          'm': 'Meeting/conference room',
          'r': 'Reception desk',
          'x': 'Exit',
          'n': 'Entrance',
          'e': 'Extras'}
# Dictionary for hashing codecs and extensions.
h_extension = {'libx264': 'mp4',
               'mpeg4': 'mp4',
               'rawvideo': 'avi',
               'png': 'avi',
               'libvorbis': 'ogv',
               'libvpx': 'webm'}
# List of all 248 country codes.
country_codes_2_letter = ['af', 'al', 'dz', 'as', 'ad', 'ao', 'ai', 'aq', 'ag',
                          'ar', 'am', 'aw', 'au', 'at', 'az', 'bs', 'bh', 'bd',
                          'bb', 'by', 'be', 'bz', 'bj', 'bm', 'bt', 'bo', 'bq',
                          'ba', 'bw', 'bv', 'br', 'io', 'bn', 'bg', 'bf', 'bi',
                          'kh', 'cm', 'ca', 'cv', 'ky', 'cf', 'td', 'cl', 'cn',
                          'cx', 'cc', 'co', 'km', 'cg', 'cd', 'ck', 'cr', 'hr',
                          'cu', 'cw', 'cy', 'cz', 'ci', 'dk', 'dj', 'dm', 'do',
                          'ec', 'eg', 'sv', 'gq', 'er', 'ee', 'et', 'fk', 'fo',
                          'fj', 'fi', 'fr', 'gf', 'pf', 'tf', 'ga', 'gm', 'ge',
                          'de', 'gh', 'gi', 'gr', 'gl', 'gd', 'gp', 'gu', 'gt',
                          'gg', 'gn', 'gw', 'gy', 'ht', 'hm', 'va', 'hn', 'hk',
                          'hu', 'is', 'in', 'id', 'ir', 'iq', 'ie', 'im', 'il',
                          'it', 'jm', 'jp', 'je', 'jo', 'kz', 'ke', 'ki', 'kp',
                          'kr', 'kw', 'kg', 'la', 'lv', 'lb', 'ls', 'lr', 'ly',
                          'li', 'lt', 'lu', 'mo', 'mk', 'mg', 'mw', 'my', 'mv',
                          'ml', 'mt', 'mh', 'mq', 'mr', 'mu', 'yt', 'mx', 'fm',
                          'md', 'mc', 'mn', 'me', 'ms', 'ma', 'mz', 'mm', 'na',
                          'nr', 'np', 'nl', 'nc', 'nz', 'ni', 'ne', 'ng', 'nu',
                          'nf', 'mp', 'no', 'om', 'pk', 'pw', 'ps', 'pa', 'pg',
                          'py', 'pe', 'ph', 'pn', 'pl', 'pt', 'pr', 'qa', 'ro',
                          'ru', 'rw', 're', 'bl', 'sh', 'kn', 'lc', 'mf', 'pm',
                          'vc', 'ws', 'sm', 'st', 'sa', 'sn', 'rs', 'sc', 'sl',
                          'sg', 'sx', 'sk', 'si', 'sb', 'so', 'za', 'gs', 'ss',
                          'es', 'lk', 'sd', 'sr', 'sj', 'sz', 'se', 'ch', 'sy',
                          'tw', 'tj', 'tz', 'th', 'tl', 'tg', 'tk', 'to', 'tt',
                          'tn', 'tr', 'tm', 'tc', 'tv', 'ug', 'ua', 'ae', 'gb',
                          'us', 'um', 'uy', 'uz', 'vu', 've', 'vn', 'vg', 'vi',
                          'wf', 'eh', 'ye', 'zm', 'zw']
# Dictionary of hashed country codes.
h_248 = list(h_676.values())[:248]
h_country = {k: v for k, v in zip(country_codes_2_letter, h_248)}
