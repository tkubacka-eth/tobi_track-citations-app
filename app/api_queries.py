import pandas as pd
import numpy as np
import requests as requests
import streamlit as st
import time
import json as json


def load_data(doi_list, db_selection, my_email_address, opencitations_access_token, semanticscholar_api_key):
    if len(doi_list) == 0:
        st.warning('Please enter at least one valid DOI or generate a random sample of DOIs')
        return 'Failure', 0
    if len(doi_list) > 20:
        st.warning('Please entre no more than 20 dois')
        return 'Failure', 0

    if not db_selection:
        st.warning('Select at least one dataset')
        return 'Failure', 0

    df = pd.DataFrame()
    step = 1
    if 'Crossref' in db_selection:
        with st.spinner(text=f"Step {step}/{len(db_selection)}: Loading Crossref data..."):
            df = pd.concat([df, get_crossref_counts(doi_list, my_email_address)])
        step += 1
    if 'OpenAlex' in db_selection:
        with st.spinner(text=f"Step {step}/{len(db_selection)}: Loading OpenAlex data..."):
            df = pd.concat([df, get_openalex_counts(doi_list, my_email_address)])
        step += 1
    if 'OpenCitations' in db_selection:
        with st.spinner(text=f"Step {step}/{len(db_selection)}: Loading OpenCitations Index data..."):
            df = pd.concat([df, get_opencitations_index_counts(doi_list, opencitations_access_token)])
            df = pd.concat([df, get_opencitations_meta_counts(doi_list, opencitations_access_token)])
        step += 1
    if 'Semantic Scholar' in db_selection:
        with st.spinner(text=f"Step {step}/{len(db_selection)}: Loading Semantic Scholar data..."):
            df = pd.concat([df, get_semanticscholar_counts(doi_list, semanticscholar_api_key)])
        step += 1
    if 'OpenAIRE' in db_selection:
        with st.spinner(text=f"Step {step}/{len(db_selection)}: Loading OpenAIRE data..."):
            df = pd.concat([df, get_openaire_counts(doi_list)])
        step += 1
    st.success('Counts successfully imported')
    return 'Success', df


@st.cache_data(show_spinner=False)
def get_openalex_sample(sample_size, institution_id, my_email_address=''):
    dois = []
    i = 0
    while (len(dois) < sample_size) & (i < 5):
        url = f"https://api.openalex.org/works"
        params = {
            'select': 'doi',
            'sample': sample_size + 5,  # some publications might not have any doi
            'per_page': sample_size + 5,
            'mailto': f'{my_email_address}'
        }
        if institution_id:
            params['filter'] = f'institutions.id:{institution_id}'
        r = requests.get(url, params=params)
        results = r.json()
        temp = [results['results'][i]['doi'] for i in range(len(results['results']))]
        dois += [x for x in temp if ((x is not None) & ~(x in dois))]
        i += 1
    return dois[0:sample_size]


@st.cache_data(show_spinner=False)
def get_crossref_counts(dois, my_email_address):
    start_time = time.time()
    url = f"https://api.crossref.org/works/"
    params = {
        f'filter': 'doi:' + ',doi:'.join(dois),
        f'select': 'DOI,is-referenced-by-count,references-count,author',
        f'mailto': f'{my_email_address}'
    }
    r = requests.get(url, params=params)
    if r.json()['status'] == 'failed':
        st.warning(r.json()['message'][0]['message'])
        return pd.DataFrame()
    all_results = r.json()['message']['items']
    df_counts = pd.DataFrame(all_results)
    if not df_counts.empty:
        df_counts['author'] = df_counts['author'].apply(lambda x: len(x) if isinstance(x, list) else np.nan)
        df_counts['DOI'] = df_counts['DOI'].str.lower()
        df_counts = df_counts.rename({'DOI': 'doi',
                                      'is-referenced-by-count': 'citations',
                                      'references-count': 'references',
                                      'author': 'authors'}, axis=1)
        df_counts = pd.melt(df_counts, 'doi', var_name='count', value_name='value')
    else:
        pass
    df_counts['database'] = 'Crossref'
    st.write(f'Crossref data loaded in %.2f seconds.' % (time.time() - start_time))
    return df_counts


@st.cache_data(show_spinner=False)
def get_openalex_counts(dois, my_email_address=''):
    start_time = time.time()
    full_dois = ['https://doi.org/' + doi for doi in dois]
    url = f"https://api.openalex.org/works"
    params = {
        'filter': f'doi:{"|".join(full_dois)}',
        'select': 'doi,cited_by_count,referenced_works,authorships',
        'mailto': f'{my_email_address}'
    }
    r = requests.get(url, params=params)
    results = r.json()['results']
    df_counts = pd.DataFrame(results)
    if not df_counts.empty:
        df_counts['referenced_works'] = df_counts['referenced_works']. \
                apply(lambda x: len(x) if isinstance(x, list) else 0)
        df_counts['authorships'] = df_counts['authorships'].apply(lambda x: len(x) if isinstance(x, list) else 0)
        df_counts = df_counts.rename({'cited_by_count': 'citations',
                                      'referenced_works': 'references',
                                      'authorships': 'authors'}, axis=1)
        df_counts = pd.melt(df_counts, 'doi', var_name='count', value_name='value')
        df_counts['doi'] = df_counts['doi'].str[16:]
        df_counts = df_counts.drop_duplicates().reset_index(drop=True)
        if len(df_counts) != 3*len(dois):
            if not df_counts[df_counts.duplicated(['doi', 'count'], keep=False)].empty:
                st.warning('Not all counts are unique in OpenAlex:')
                st.write(df_counts[df_counts.duplicated(['doi', 'count'], keep=False)])
                st.warning('For each count, only one value has been kept.')
                df_counts = df_counts.drop_duplicates(['doi', 'count']).reset_index(drop=True)
    else:
        pass
    df_counts['database'] = 'OpenAlex'
    st.write(f'OpenAlex data loaded in %.2f seconds.' % (time.time() - start_time))
    return df_counts


@st.cache_data(show_spinner=False)
def get_opencitations_index_counts(dois, opencitations_access_token=''):
    """Returns a df containing counts for citation, author and reference.
    In the case where there is no citation or reference,
    counts for those metadata are set to 0 when some metadata is associated to the doi,
    to nan where there is no metadata"""
    start_time = time.time()
    headers = {"authorization": f"{opencitations_access_token}"}
    citations = []
    references = []
    authors = []
    for doi in dois:
        url = 'https://opencitations.net/index/api/v2/citation-count/doi:' + doi
        r = requests.get(url, headers=headers)
        if r and len(r.json()) > 0:
            citations += [int(r.json()[0]['count'])]
        else:
            citations += [np.nan]
        url = 'https://opencitations.net/index/api/v2/reference-count/doi:' + doi
        r = requests.get(url, headers=headers)
        if r and len(r.json()) > 0:
            references += [int(r.json()[0]['count'])]
        else:
            references += [np.nan]
    df_counts = pd.DataFrame({"doi": dois,
                              "citations": citations,
                              "references": references})
    df_counts = pd.melt(df_counts, 'doi', var_name='count', value_name='value')
    df_counts['database'] = 'OpenCitations'
    st.write(f'*Citation-count* and *reference-count* queries of OpenCitations Index data '
             f'loaded in %.2f seconds.' % (time.time() - start_time))
    # st.write(f'OpenCitations Index data loaded in %.2f seconds.' % (time.time() - start_time))
    return df_counts


@st.cache_data(show_spinner=False)
def get_opencitations_meta_counts(dois, opencitations_access_token=''):
    start_time = time.time()
    headers = {"authorization": f"{opencitations_access_token}"}
    
    records = []
    
    for doi in dois:
        url = f'https://opencitations.net/meta/api/v1/metadata/doi:{doi}'
        try:
            r = requests.get(url, headers=headers)
            r.raise_for_status()
            result = r.json()

            if not isinstance(result, list) or not result:
                continue

            metadata = result[0]  # assume first record is most relevant
            record = {
                'doi': doi,
                'authors': metadata.get('author', '').count(';') + (1 if metadata.get('author', '') else 0)
            }
            records.append(record)

        except requests.RequestException as e:
            st.warning(f"Request error for DOI {doi}: {e}")
        except Exception as e:
            st.warning(f"Unexpected error for DOI {doi}: {e}")

    if not records:
        return pd.DataFrame()

    df_counts = pd.DataFrame(records)
    df_counts = pd.melt(df_counts, id_vars='doi', var_name='count', value_name='value')
    df_counts['database'] = 'OpenCitations'

    st.write(f'*Metadata* queries from OpenCitations Meta completed in %.2f seconds.' % (time.time() - start_time))
    return df_counts

@st.cache_data(show_spinner=False)
def get_semanticscholar_counts(dois, semanticscholar_api_key=''):
    start_time = time.time()
    headers = {"x-api-key": f"{semanticscholar_api_key}"}
    url = f"https://api.semanticscholar.org/graph/v1/paper/batch"
    params = {
        'fields': 'referenceCount,citationCount,authors,externalIds',
    }
    dois = [('ARXIV:' + doi[15:]) if doi[:15] == '10.48550/arxiv.' else doi for doi in dois]
    data = json.dumps({"ids": dois})
    r = requests.post(url, headers=headers, params=params, data=data)
    all_results = r.json()
    if not ((str(all_results)[2:7] == 'error') | (str(all_results)[2:9] == 'message')):
        all_results = [x for x in all_results if x is not None]
        df_counts = pd.DataFrame(all_results)
        external_ids = df_counts['externalIds'].apply(pd.Series)
        if 'DOI' in external_ids.columns:
            if 'ArXiv' in external_ids.columns:
                external_ids['ArXiv'] = '10.48550/arxiv.' + external_ids['ArXiv']
                external_ids = external_ids['DOI'].fillna(external_ids['ArXiv'])
            else:
                external_ids = external_ids['DOI']
        else: # 'ArXiv' in external_ids.columns
            external_ids = '10.48550/arxiv.' + external_ids['ArXiv']
        df_counts['doi'] = external_ids.apply(lambda x: x.lower())
        df_counts['authors'] = df_counts['authors'].apply(lambda x: len(x))
        df_counts = df_counts[['doi', 'citationCount', 'referenceCount', 'authors']]
        df_counts = df_counts.rename({'citationCount': 'citations',
                                      'referenceCount': 'references'}, axis=1)
        df_counts = pd.melt(df_counts, 'doi', var_name='count', value_name='value')
    else:
        if str(all_results)[2:9] == 'message':
            st.write('Message from Semantic Scholar: "', all_results['message'], '"')
            df_counts = pd.DataFrame()
        else:
            df_counts = pd.DataFrame()
    df_counts['database'] = 'Semantic Scholar'
    st.write(f'Semantic Scholar data loaded in %.2f seconds.' % (time.time() - start_time))
    return df_counts


def get_openaire_dollar(dict_or_list):
    if type(dict_or_list) is dict:
        return dict_or_list['$']
    else:
        return dict_or_list[0]['$'] # takes only first of the list

def get_openaire_dois_list(dict_or_list, dois):
    if type(dict_or_list) is dict:
        return [dict_or_list['$']]
    else:
        dois_list = [dict_or_list[i]['$'] for i in range(len(dict_or_list))]
        dois_list = [x for x in dois_list if x in dois]
        return dois_list


@st.cache_data(show_spinner=False)
def get_openaire_counts(dois):
    start_time = time.time()
    base_url = "https://api.openaire.eu/graph/v1/researchProducts"
    headers = {"Accept": "application/json"}

    all_records = []

    for doi in dois:
        params = {
            "pid": doi
        }

        try:
            r = requests.get(base_url, params=params, headers=headers)
            r.raise_for_status()
            result = r.json()
            if result:
                record = result[0] if isinstance(result, list) else result
                all_records.append({
                    "doi": doi.lower(),
                    "citations": record.get("citationCount"),
                    "references": record.get("referenceCount"),
                    "authors": record.get("authorCount")
                })
        except Exception as e:
            st.warning(f"OpenAIRE error for DOI {doi}: {e}")

    if not all_records:
        return pd.DataFrame()

    df_counts = pd.DataFrame(all_records)
    df_counts = pd.melt(df_counts, id_vars='doi', var_name='count', value_name='value')
    df_counts['database'] = 'OpenAIRE'
    st.write(f'OpenAIRE Graph API data loaded in %.2f seconds.' % (time.time() - start_time))
    return df_counts

@st.cache_data(show_spinner=False)
def get_datacite_counts(dois):
    start_time = time.time()
    url = 'https://api.datacite.org/dois'
    # https: // api.datacite.org / dois?query = (doi:10.11570 / 18.0007 % 20OR % 20doi:10.11570 / 18.0006)
    params = {
        'query': '(doi:' + ' OR doi:'.join(dois) + ')'
    }
    r = requests.get(url, params=params)
    results = r.json()
    df_counts = pd.DataFrame(results['data'])
    st.write(df_counts)
    st.write(f'DataCite data loaded in %.2f seconds.' % (time.time() - start_time))
    return 0
