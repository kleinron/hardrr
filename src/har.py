"""
this module assumes that the response's body is *always* stored in its binary format. that is, payload is always encoded
in base64. code is not written nor tested with other (acceptable) scenarios.
"""

import datetime
import urlparse
import base64
import json
import traceback


DATE_TIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'


def _datetime_to_unicode(dt):
    return dt.strftime(DATE_TIME_FORMAT).decode()


def ensure_and_convert_to_utf8(v, name):
    try:
        _ = unicode(v).encode('utf-8')
        return v
    except:
        pass

    try:
        new_v = v.decode('utf-8').encode('utf-8')
        return new_v
    except:
        pass

    try:
        new_v = v.decode('latin1').encode('utf-8')
        return new_v
    except:
        raise ValueError('cannot encode variable %s to utf-8' % name)


class TransformError(Exception):
    def __init__(self, starting_url, inner_exception, inner_trace):
        assert isinstance(inner_exception, Exception)
        assert isinstance(starting_url, (str, unicode))
        self.starting_url = starting_url
        self.inner_exception = inner_exception
        self.inner_trace = inner_trace

        if self.starting_url is None:
            self.starting_url = 'unknown'
        else:
            self.starting_url = str(self.starting_url)

    def __str__(self):
        return 'starting_url = %s, inner_exception = %s, inner_trace:\n%s' % (self.starting_url, str(self.inner_exception), self.inner_trace)

    __repr__ = __str__


class NameValueComment(dict):
    def __init__(self, name, value, comment=None, **kwargs):
        super(NameValueComment, self).__init__(**kwargs)
        assert name is not None
        assert value is not None
        self['name'] = ensure_and_convert_to_utf8(name, 'name')
        self['value'] = ensure_and_convert_to_utf8(value, 'value (of \'%s\')' % name)
        if comment is not None:
            self['comment'] = ensure_and_convert_to_utf8(comment, 'comment')


    def __radd__(self, other):
        return other + len(self['name']) + len(self['value'])
    
    def __unicode__(self):
        return unicode(json.dumps(self))

    @classmethod
    def from_dict(cls, d):
        if 'comment' not in d:
            d['comment'] = None
        return cls(**d)
    
    @classmethod
    def from_args(cls, *args):
        return cls(*args)


class Header(NameValueComment):
    _joiner = ': '.join
    
    def __init__(self, name, value, comment=None):
        NameValueComment.__init__(self, name, value, comment)
    
    def to_line(self):
        result = self._joiner((self['name'], self['value']))
        return result

    def calculate_size(self):
        return len(self.to_line())


class QueryStringItem(NameValueComment):
    pass


class NameBase(dict):
    def __init__(self, name, version, comment, **kwargs):
        super(NameBase, self).__init__(**kwargs)
        self['name'] = ensure_and_convert_to_utf8(name, 'name')
        self['version'] = ensure_and_convert_to_utf8(version, 'version')
        if comment is not None:
            self['comment'] = ensure_and_convert_to_utf8(comment, 'comment')
        
    def __unicode__(self):
        return unicode(json.dumps(self))
        

class Creator(NameBase):
    def __init__(self, name='', version='1.2', comment=None):
        NameBase.__init__(self, name=name, version=version, comment=comment)


class Browser(NameBase):
    def __init__(self, name='', version='1.2', comment=None):
        NameBase.__init__(self, name=name, version=version, comment=comment)


TIMINGS_DEFAULT_VALUE = -1


class Timings(dict):
    def __init__(self, blocked=TIMINGS_DEFAULT_VALUE, dns=TIMINGS_DEFAULT_VALUE, connect=TIMINGS_DEFAULT_VALUE,
                 send=TIMINGS_DEFAULT_VALUE, wait=TIMINGS_DEFAULT_VALUE, receive=TIMINGS_DEFAULT_VALUE,
                 ssl=TIMINGS_DEFAULT_VALUE, comment=None, **kwargs):
        super(Timings, self).__init__(**kwargs)
        self['blocked'] = blocked
        self['dns'] = dns
        self['connect'] = connect
        self['send'] = send
        self['wait'] = wait
        self['receive'] = receive
        self['ssl'] = ssl
        if comment is not None:
            self['comment'] = comment
    
    @classmethod
    def empty(cls):
        return cls(blocked=TIMINGS_DEFAULT_VALUE, dns=TIMINGS_DEFAULT_VALUE, connect=TIMINGS_DEFAULT_VALUE, send=TIMINGS_DEFAULT_VALUE, wait=TIMINGS_DEFAULT_VALUE, receive=TIMINGS_DEFAULT_VALUE, ssl=TIMINGS_DEFAULT_VALUE, comment=None)


class PostData(dict):
    def __init__(self, mimeType, params=None, text=None, comment=None, **kwargs):
        super(PostData, self).__init__(**kwargs)
        self['mimeType'] = mimeType
        if params is not None:
            self['params'] = params
        elif text is not None:
            self['text'] = text
        if comment is not None:
            self['comment'] = comment
    

class Response(dict):
    # noinspection PyPep8Naming
    def __init__(self, status, statusText, httpVersion, cookies, headers, content, redirectURL, headersSize, bodySize, comment, **kwargs):
        super(Response, self).__init__(**kwargs)
        self['status'] = status
        self['statusText'] = statusText
        self['httpVersion'] = httpVersion
        self['cookies'] = cookies
        self['headers'] = headers
        self['content'] = content
        if redirectURL is None:
            redirectURL = ''
        self['redirectURL'] = redirectURL
        self['headersSize'] = headersSize
        self['bodySize'] = bodySize
        if comment is not None:
            self['comment'] = comment
            
    def try_get_header_by_name(self, name, default_value=None):
        headers = self['headers']
        if headers is None:
            return default_value
        filtered = filter(lambda x: x['name'].lower().strip() == name.lower(), headers)
        if len(filtered) != 0:
            return filtered[0]
        
        return default_value
    
    def headers_as_dict(self, convert_keys_casing_to=None):
        result = {}
        if convert_keys_casing_to is None:
            case_func = lambda x: x
        elif convert_keys_casing_to.lower() == 'lower':
            case_func = lambda x: x.lower()
        elif convert_keys_casing_to.lower() == 'upper':
            case_func = lambda x: x.upper()
        else:
            raise ValueError('"convert_keys_casing_to" can be: None, "lower" or "upper"')
        
        headers = self['headers']
        # ignore duplicates, take last item in such case
        for header in headers:
            result[case_func(header['name'])] = header['value']
        return result
            
    @staticmethod
    def from_dict(d):
        if d is None: # relevant for robots disallowed
            return None
        content = Content.from_dict(d['content'])
        d['content'] = content
        if 'comment' not in d:
            d['comment'] = None
        headers = d['headers']
        headers = map(Header.from_dict, headers)
        d['headers'] = headers
        return Response(**d)


class Content(dict):
    def __init__(self, size, compression, mimeType, text, comment, **kwargs):
        super(Content, self).__init__(**kwargs)
        self['size'] = size
        self['compression'] = compression
        self['mimeType'] = mimeType
        self['text'] = base64.standard_b64encode(text or '')
        if comment is not None:
            self['comment'] = comment
        self['encoding'] = 'base64'
        
    def try_get_content(self, default_value=None):
        t = self.get('text')
        if t is None:
            return default_value
        return base64.standard_b64decode(t)
        
    def as_empty_text(self):
        result = Content(self['size'], self['compression'], self['mimeType'], '', self.get('comment', None))
        return result
    
    @staticmethod
    def from_dict(d):
        del d['encoding']
        d['text'] = base64.standard_b64decode(d['text'])
        if not 'comment' in d:
            d['comment'] = None
        return Content(**d)


def _bool_to_unicode(b):
    return str(b).decode().lower()


class Cookie(NameValueComment):
    def __init__(self, name, value, path=None, domain=None, expires=None, httpOnly=None, secure=None, comment=None):
        NameValueComment.__init__(self, name, value, comment)
        if path is not None:
            self['path'] = path
        if domain is not None:
            self['domain'] = domain
        if expires is not None:
            self['expires'] = expires
        if expires is not None:
            assert type(expires) is datetime.datetime
            self['expires'] = _datetime_to_unicode(expires)
        if httpOnly is not None:
            assert type(httpOnly) is bool
            self['httpOnly'] = _bool_to_unicode(httpOnly)
        if secure is not None:
            assert type(secure) is bool
            self['secure'] = _bool_to_unicode(secure)


def parse_qsl(qs, keep_blank_values=0, strict_parsing=0):
    """copied from urlparse.parse_qsl, 
    modifying only the unquote function to have no impact what so ever.
    this means that percent-escaping remains as is.
    """
    
    unquote = lambda x: x
    
    pairs = [s2 for s1 in qs.split('&') for s2 in s1.split(';')]
    r = []
    for name_value in pairs:
        if not name_value and not strict_parsing:
            continue
        nv = name_value.split('=', 1)
        if len(nv) != 2:
            if strict_parsing:
                raise ValueError, "bad query field: %r" % (name_value,)
            # Handle case of a control-name with no equal sign
            if keep_blank_values:
                nv.append('')
            else:
                continue
        if len(nv[1]) or keep_blank_values:
            name = unquote(nv[0].replace('+', ' '))
            value = unquote(nv[1].replace('+', ' '))
            r.append((name, value))

    return r            


class Request(dict):
    # noinspection PyPep8Naming
    def __init__(self, method, url, httpVersion, cookies=None, headers=None, queryString=None, postData=None,
                 headersSize=-1, bodySize=-1, comment=None, **kwargs):
        super(Request, self).__init__(**kwargs)
        if not queryString:
            queryString = []
        if not headers:
            headers = []
        if not cookies:
            cookies = []
        self['method'] = method
        self['url'] = url.strip()
        self['httpVersion'] = httpVersion
        self['cookies'] = cookies
        self['headers'] = headers
        if queryString is not None:
            self['queryString'] = queryString
        else:
            parsed_url = urlparse.urlparse(self['url'])
            query = parsed_url.query
            if query is None or len(query) == 0:
                self['queryString'] = []
            else:
                parsed_query = parse_qsl(query, keep_blank_values=True)
                self['queryString'] = map(QueryStringItem.from_args, parsed_query)
        if postData is not None:
            self['postData'] = postData
        if headersSize is not None and headersSize >= 0:
            self['headersSize'] = headersSize
        elif headers is not None:
            sum_headers = sum(headers) if len(headers) != 0 else 0
            self['headersSize'] = sum_headers + (len(headers) * len('\r\n'))
        else:
            self['headersSize'] = -1
        self['bodySize'] = bodySize
        if comment is not None:
            self['comment'] = comment


class Entry(dict):
    # noinspection PyPep8Naming
    def __init__(self, startedDateTime, request, response, cache, timings, serverIPAddress=None, connection=None, pageref=None, comment=None, **kwargs):
        super(Entry, self).__init__(**kwargs)
        if pageref is not None:
            self['pageref'] = pageref
        if startedDateTime is not None:
            if type(startedDateTime) is datetime.datetime:
                startedDateTime = _datetime_to_unicode(startedDateTime)
            self['startedDateTime'] = startedDateTime
        self['time'] = 0
        self['request'] = request
        self['response'] = response
        if cache is None:
            cache = {}
        self['cache'] = cache
        self['timings'] = timings
        if serverIPAddress is not None:
            self['serverIPAddress'] = serverIPAddress
        if connection is not None:
            self['connection'] = connection
        if comment is not None:
            self['comment'] = comment
            
    def read_started_date_time(self):
        return datetime.datetime.strptime(self['startedDateTime'], "%Y-%m-%dT%H:%M:%S.%fZ")
    
    @staticmethod
    def from_dict(d):
        response = Response.from_dict(d['response'])
        d['response'] = response
        # del(d['time'])
        return Entry(**d)

    def has_non_empty_response(self):
        response = self.get('response', None)
        if response is None:
            return False

        content = response.get('content', None)
        if content is None:
            return False

        if not isinstance(content, Content):
            return False

        response_bytes = content.get('text', None)
        if response_bytes is None:
            return False

        # noinspection PyTypeChecker
        if len(response_bytes) == 0:
            return False

        return True

    def robots_disallowed(self):
        """
        return True if this entry has a comment, and in this comment we can see our indication of robots=disallowed
        otherwise, return False
        """
        comment = self.get('comment', None)
        if comment is None:
            return False
        try:
            pairs = urlparse.parse_qsl(comment)
        except:
            # cannot parse the comment
            return False

        robots = filter(lambda x: x[0] == 'robots', pairs)
        if len(robots) == 0:
            return False

        # assume that only a single item exists, ignore array-like query string values, take the first one
        value = robots[0][1]
        if value is None:
            return False

        if value != 'disallowed':
            return False

        # we found "robots=disallowed", so that's that...
        return True


class Log(dict):
    def __init__(self, version=None, creator=None, browser=None, pages=None, entries=None, comment=None, **kwargs):
        super(Log, self).__init__(**kwargs)
        if not pages:
            pages = []
        if not entries:
            entries = []
        if version is None:
            version = "1.2"
        self['version'] = version
        self['creator'] = creator
        self['browser'] = browser
        self['pages'] = pages
        self['entries'] = entries
        if comment is not None:
            self['comment'] = comment
    
    @staticmethod
    def from_dict(d):
        entries = map(Entry.from_dict, d['entries'])
        d['entries'] = entries
        return Log(**d)
    
    @staticmethod
    def load(fd):
        return Log.loads(fd.read().decode('utf-8'))
    
    @staticmethod
    def combine_to_first(har_logs):
        if not har_logs:
            return None

        first_har_log = har_logs[0]
        entries = first_har_log['entries']
        for har_log in har_logs[1:]:
            for e in har_log['entries']:
                entries.append(e)

        return first_har_log

    @staticmethod
    def combine_from(directory_path, output_path=None):
        import os
        har_logs = []
        for file_name in os.listdir(directory_path):
            if not file_name.endswith('.har'):
                continue
            file_path = os.path.join(directory_path, file_name)
            har_logs.append(Log.read_from(file_path))

        result = Log.combine_to_first(har_logs)
        if output_path is not None:
            result.save_as(output_path)

        return result

    @staticmethod
    def read_from(file_path):
        with open(file_path, 'r') as fd:
            return Log.loads(unicode(fd.read()))
                
    @staticmethod
    def loads(s):
        if type(s) is unicode:
            s = s.encode('utf-8')
        if len(s) == 0:
            raise ValueError('cannot parse an empty string as HAR')
        return Log.from_dict(json.loads(s)['log'])
    
    def save_as(self, file_path, on_start_write=None, on_end_write=None):
        if on_start_write is None:
            on_start_write = lambda: None
        if on_end_write is None:
            on_end_write = lambda: None

        content = self.__unicode__().encode('utf-8')
        on_start_write()
        with open(file_path, 'w') as f:
            f.write(content)
        on_end_write()
            
    def dumps(self, encoding='utf-8'):
        return json.dumps({u'log': self}, encoding=encoding)
    
    def __unicode__(self):
        try:
            return unicode(json.dumps({u'log': self}))
        except Exception as ex:
            try:
                inner_trace = traceback.format_exc()
            except:
                inner_trace = 'cannot extract inner trace'
            try:
                url = self['entries'][0]['request']['url']
            except:
                url = 'unknown'
            raise TransformError(url, ex, inner_trace)

    def extract_payload_of_last_entry(self, default_value=''):
        last_entry = self.get_last_entry()
        last_entry_response = last_entry['response']
        if last_entry_response is None:
            return default_value
        last_entry_content_node = last_entry_response['content']
        if last_entry_content_node is None:
            return default_value
        payload = last_entry_content_node.try_get_content(default_value=default_value)
        return payload

    def get_last_entry(self):
        last_entry = self['entries'][-1]
        return last_entry

    
    @staticmethod
    def create_empty(creator=None, default_comment_for_all_elements=''):
        result = Log(version=None, creator=creator, browser=Browser(comment=default_comment_for_all_elements), pages=[], entries=[], comment=default_comment_for_all_elements)
        return result

if __name__ == '__main__':
    pass