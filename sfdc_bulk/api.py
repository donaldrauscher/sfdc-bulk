

# Interface to the Salesforce BULK API
import csv, re, time, logging, requests
import xml.etree.ElementTree as ET
import pandas as pd
from io import StringIO
from collections import OrderedDict
from simple_salesforce import SalesforceLogin


ERROR_STATES = ('Failed','Not Processed')
PENDING_STATES = ('InProcess', 'Queued')
COMPLETED_STATES = ('Completed')


class BulkApiError(Exception):

    def __init__(self, message, logger):
        logger.exception(message)
        super(BulkApiError, self).__init__(message)

class BulkBatchFailed(Exception):

    def __init__(self, job_id, batch_id, state_message, logger):
        self.job_id = job_id
        self.batch_id = batch_id
        self.state_message = state_message

        message = 'Batch %s of job %s failed: %s' % (batch_id, job_id, state_message)
        logger.exception(message)
        super(BulkBatchFailed, self).__init__(message)


class SalesforceBulkAPI(object):

    jobNS = 'http://www.force.com/2009/06/asyncapi/dataload'
    jobs = []
    job_statuses = {}
    batches = {}  # dict of job_id => [batch_id, batch_id, ...]
    batch_statuses = {}

    def __init__(self, username=None, password=None, security_token=None, organization_id=None, sandbox=False, API_version="37.0", batch_size=5000, logger=None, verbose=True):

        # use SalesforceLogin from simple_salesforce for authentication
        self.session_id, host = SalesforceLogin(username=username, password=password, security_token=security_token, organizationId=organization_id, sandbox=sandbox)
        host = "https://" + host if host[0:4] != 'http' else host
        endpoint = host.replace(".salesforce.com", "-api.salesforce.com")
        endpoint += "/services/async/%s" % API_version
        self.endpoint = endpoint

        self.batch_size = batch_size

        if not logger:
            logger = logging.getLogger(__name__)
            logger.setLevel(logging.DEBUG)
            if verbose:
                h = logging.StreamHandler()
                h.setFormatter(logging.Formatter("%(levelname)s - %(message)s"))
                logger.addHandler(h)
            else:
                logger.addHandler(logging.NullHandler())
        self._logger = logger

    # some utility functions
    def headers(self, values={}):
        default = {"X-SFDC-Session": self.session_id, "Content-Type": "application/xml; charset=UTF-8"}
        for k, val in values.items():
            default[k] = val
        return default

    def raise_error(self, message, status_code=None):
        if status_code:
            message = "[%s] %s" % (status_code, message)

        raise BulkApiError(message, self._logger)

    def check_status(self, status_code, content):
        if status_code >= 400:
            msg = "Bulk API HTTP Error result: %s" % (content)
            self.raise_error(msg, status_code)

    def lookup_job_id(self, batch_id):
        for job_id in self.batches.keys():
            if batch_id in self.batches[job_id]:
                return job_id

        raise Exception("Batch id '%s' is uknown, can't retrieve job_id" % batch_id)

    def parse_xml(self, xml):
        tree = ET.fromstring(xml)
        result = {}
        for child in tree:
            result[re.sub("{.*?}", "", child.tag)] = child.text
        return result

    def df_chunks(self, df):
        chunks = list()
        n_chunks = len(df) // self.batch_size + 1
        for i in range(n_chunks):
            chunks.append(df.loc[i*self.batch_size:(i+1)*self.batch_size,:])
        return chunks

    # some methods for monitoring jobs
    def get_status(self, obj_id, obj_type='job', reload=False):
        if obj_type == 'job':
            if not reload and obj_id in self.job_statuses:
                return self.job_statuses[obj_id]
            url = self.endpoint + "/job/%s" % obj_id
        else:
            if not reload and obj_id in self.batch_statuses:
                return self.batch_statuses[obj_id]
            job_id = self.lookup_job_id(obj_id)
            url = self.endpoint + "/job/%s/batch/%s" % (job_id, obj_id)

        req = requests.get(url, headers=self.headers())
        self.check_status(req.status_code, req.text)

        req2 = self.parse_xml(req.text)
        self.job_statuses[obj_id] = req2
        return req2

    def is_batch_done(self, batch_id):
        if batch_id in self.batch_statuses:
            if self.batch_statuses[batch_id]['state'] in COMPLETED_STATES:
                self._logger.debug("Batch id %s completed previously." % (batch_id))
                return True

        batch_status = self.get_status(batch_id, 'batch', reload=True)
        batch_state = batch_status['state']

        if batch_state in ERROR_STATES:
            raise BulkBatchFailed(self.lookup_job_id(batch_id), batch_id, batch_status['stateMessage'], self._logger)

        is_complete = batch_state in COMPLETED_STATES

        self._logger.debug("Batch id %s is %scomplete." % (batch_id, '' if is_complete else 'not '))

        return is_complete

    def is_job_done(self, job_id):
        try:
            is_completed = True
            for i, batch_id in enumerate(self.batches[job_id]):
                self._logger.debug("Checking status of batch id %s... (%s/%s)" % (batch_id, i+1, len(self.batches[job_id])))
                if not self.is_batch_done(batch_id):
                    self._logger.debug("Exiting loop.")
                    is_completed = False
                    break
            return is_completed
        except KeyError:
            raise Exception("Job id '%s' does not have any batches!" % job_id)

    def wait_for_batch(self, batch_id, timeout=60*10, sleep_interval=10):
        waited = 0
        while not self.is_batch_done(batch_id) and waited < timeout:
            self._logger.debug("Waiting...")
            time.sleep(sleep_interval)
            waited += sleep_interval

    def wait_for_job(self, job_id, timeout=60*60, sleep_interval=30):
        waited = 0
        while not self.is_job_done(job_id) and waited < timeout:
            self._logger.debug("Waiting...")
            time.sleep(sleep_interval)
            waited += sleep_interval


    # makes a job (query, insert, upsert, update, or delete)
    def create_query_job(self, **kwargs):
        return self.create_job(operation="query", **kwargs)

    def create_insert_job(self, **kwargs):
        return self.create_job(operation="insert", **kwargs)

    def create_upsert_job(self, **kwargs):
        return self.create_job(operation="upsert", **kwargs)

    def create_update_job(self, **kwargs):
        return self.create_job(operation="update", **kwargs)

    def create_delete_job(self, **kwargs):
        return self.create_job(operation="delete", **kwargs)

    def create_hard_delete_job(self, **kwargs):
        return self.create_job(operation="hardDelete", **kwargs)

    def create_job(self, **kwargs):
        assert(kwargs['object'] is not None)
        assert(kwargs['operation'] is not None)
        if kwargs['operation'] == 'upsert':
            assert(kwargs['externalIdFieldName'] is not None)

        url = self.endpoint + "/job"
        doc = self.create_job_doc(**kwargs)
        req = requests.post(url, headers=self.headers(), data=doc)
        self.check_status(req.status_code, req.text)

        job_id = self.parse_xml(req.text)['id']
        self.jobs.append(job_id)
        self.batches[job_id] = []

        self._logger.debug("Created %s job for %s object.  Job id %s." % (kwargs['operation'], kwargs['object'], job_id))

        return job_id

    def create_job_doc(self, **kwargs):
        root = ET.Element("jobInfo")
        root.set("xmlns", self.jobNS)

        # order matters...
        kwargs_ordered = OrderedDict()
        param_order = ['operation', 'object', 'externalIdFieldName', 'concurrencyMode', 'contentType']
        for p1 in [i for i in param_order if i in kwargs.keys()]:
            kwargs_ordered[p1] = kwargs[p1]
        for p2 in [i for i in kwargs.keys() if i not in param_order]:
            kwargs_ordered[p2] = kwargs[p2]

        for key, value in kwargs_ordered.items():
            child = ET.SubElement(root, key)
            child.text = value

        doc = ET.tostring(root, encoding="UTF-8")
        doc = b'<?xml version="1.0" encoding="UTF-8"?>' + doc
        return doc


    # closes a job
    def close_job(self, job_id):
        url = self.endpoint + "/job/%s" % job_id
        req = requests.post(url, headers=self.headers(), data=self.create_job_doc(state='Closed'))
        self.check_status(req.status_code, req.text)

        self._logger.debug("Closed job id %s." % (job_id))

    # aborts a job
    def abort_job(self, job_id):
        url = self.endpoint + "/job/%s" % job_id
        req = requests.post(url, headers=self.headers(), data=self.create_job_doc(state='Aborted'))
        self.check_status(req.status_code, req.text)

        self._logger.debug("Aborted job id %s." % (job_id))


    # methods for running a query and downloading the results
    def query(self, soql):

        self._logger.debug("SOQL query to execute: %s" % (soql))

        job_id = self.create_query_job(object=re.search(re.compile("FROM (\w+)", re.I), soql).group(1), contentType='CSV')

        url = self.endpoint + "/job/%s/batch" % job_id
        req = requests.post(url, headers=self.headers({"Content-Type": "text/csv; charset=UTF-8"}), data=soql)
        self.check_status(req.status_code, req.text)

        tree = ET.fromstring(req.text)
        batch_id = tree.findtext("{%s}id" % self.jobNS)

        self._logger.debug("Job id for query is %s. Batch id is %s." % (job_id, batch_id))

        self.close_job(job_id) # can close it out (no more batches being added)

        self.batches[job_id].append(batch_id)
        return job_id

    def get_result_ids_for_query(self, job_id):
        batch_id = self.batches[job_id][0]
        self.wait_for_batch(batch_id)

        url = self.endpoint + "/job/%s/batch/%s/result" % (job_id, batch_id)
        req = requests.get(url, headers=self.headers())
        self.check_status(req.status_code, req.text)

        tree = ET.fromstring(req.text)
        result_ids = [str(x.text) for x in tree.iterfind("{%s}result" % self.jobNS)]

        self._logger.debug("Query result split across %s results: %s." % (len(result_ids), ', '.join(result_ids)))

        return result_ids

    def get_query_result(self, job_id, batch_id, result_id):

        self._logger.debug("Downloading result id %s..." % (result_id))

        url = self.endpoint + "/job/%s/batch/%s/result/%s" % (job_id, batch_id, result_id)
        req = requests.get(url, headers=self.headers())
        self.check_status(req.status_code, req.text)

        self._logger.debug("Download complete.")

        result = pd.read_csv(StringIO(req.text))
        return(result)

    def get_all_query_results(self, job_id):
        batch_id = self.batches[job_id][0]
        result_ids = self.get_result_ids_for_query(job_id)
        results = [self.get_query_result(job_id, batch_id, result_id) for result_id in result_ids]

        job_status = self.get_status(job_id, 'job')
        self._logger.debug("=====")
        self._logger.debug("Results summary for job id %s:" % (job_id))
        for k, v in job_status.items():
            self._logger.debug('    %s: %s' % (k, v))

        return pd.concat(results, axis=0)

    def bulk_csv_operation(self, job_id, in_df, close_job=True):
        url = self.endpoint + "/job/%s/batch" % job_id
        num_batches = 0

        for df_chunk in self.df_chunks(in_df):

            num_batches += 1

            buf = StringIO(newline='\n')
            df_chunk.to_csv(buf, index=False)
            buf.seek(0)

            req = requests.post(url, headers=self.headers({"Content-Type": "text/csv; charset=UTF-8"}), data=buf.read().encode('UTF-8'))
            self.check_status(req.status_code, req.text)

            tree = ET.fromstring(req.text)
            batch_id = tree.findtext("{%s}id" % self.jobNS)

            self._logger.debug("Added batch id %s (#%s) to job id %s..." % (batch_id, num_batches, job_id))

            self.batches[job_id].append(batch_id)

        if close_job:
            self.close_job(job_id)

        return self.batches[job_id]

    def get_bulk_csv_operation_result(self, job_id, batch_id):

        self._logger.debug("Downloading results for batch id %s..." % (batch_id))

        batch_status = self.get_status(batch_id, 'batch')
        self._logger.debug("Results summary for batch id %s:" % (batch_id))
        for k, v in batch_status.items():
            self._logger.debug('    %s: %s' % (k, v))

        url = self.endpoint + "/job/%s/batch/%s/result" % (job_id, batch_id)
        req = requests.get(url, headers=self.headers())
        self.check_status(req.status_code, req.text)

        self._logger.debug("Download complete.")

        result = pd.read_csv(StringIO(req.text))
        return result

    def get_bulk_csv_operation_results(self, job_id):

        self.wait_for_job(job_id)
        results = [self.get_bulk_csv_operation_result(job_id, batch_id) for batch_id in self.batches[job_id]]

        job_status = self.get_status(job_id, 'job')
        self._logger.debug("=====")
        self._logger.debug("Results summary for job id %s:" % (job_id))
        for k, v in job_status.items():
            self._logger.debug('    %s: %s' % (k, v))

        return pd.concat(results, axis=0)
