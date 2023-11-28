import pandas as pd
import numpy as np
import requests as requests
import streamlit as st
import time
import json as json
import random as random

import api_queries as api
import viz as viz


@st.cache_data()
def get_random_institution(df0):
    temp = random.randrange(len(df0))
    return df0['institution_id'][temp], df0['institution_name'][temp]


def format_doi_list(doi_list):
    """Input: a list of DOIs. Output: same list of DOIs in short form without duplicates and in lower cases"""
    doi_list = [doi for doi in doi_list if ('10.' in doi)]  # Only keep elements which contain 10.
    doi_list = [doi[doi.find('10.'):].lower() for doi in doi_list]  # Short DOI format
    doi_list = list(dict.fromkeys(doi_list))  # remove duplicates
    return doi_list


def prepare_df(df0, doi_list):
    """Input: a df and a list of DOIs.
    Output: a df, a list of databases, a pivoted df"""
    if df0.empty:
        st.warning('There is no data associated to the input')
        return df0, [], df0
    else:
        df0 = pd.merge(pd.DataFrame(doi_list, columns=['doi']).reset_index(),
                       df0,
                       how='outer', on='doi')
        df0['doi'] = 'https://doi.org/' + df0['doi']
        df0['value'] = df0['value'].astype('float')

        databases0 = df0['database'].unique().tolist()

        df0_pivoted = df0.pivot(columns='database',
                                index=['index', 'doi', 'count'],
                                values='value').reset_index().set_index('index')

        # Sort database by citations mean
        databases0 = df0_pivoted[df0_pivoted['count'] == 'citations'][databases0] \
            .dropna() \
            .mean(axis=0, skipna=True) \
            .sort_values(ascending=False) \
            .index.tolist()
        return df, databases0, df0_pivoted


def generate_tab(df0, count):
    st.header(f'{count.capitalize()} count')

    cols = st.columns([4, 1], gap='large')
    # viz.plot_rel_count_sns(df_pivoted, databases, count)
    # st.subheader('Your data table')
    viz.write_count_table(df0, databases, count_category=count, cols=cols)
    viz.plot_rel_count_plotly(df0, databases, count)
    st.header('Why are counts from open data sources different?')
    st.write('Counts may highly vary from one data source to another. '
             'While smaller counts may be associated to lower coverage, '
             'they may also be the consequence of '
             'an efficient use of *disambiguation techniques* '
             '(e.g. authors, references or citations are deduplicated), '
             'a *selective data filtering* '
             '(e.g. excluding identified paper mills or '
             'only taking open citations into account) '
             'or a *restrictive counting process* '
             '(e.g. not taking self-citations into account or '
             'counting a preprint and its associated publication as one instead of two). ')
    st.write('This app enables to observe the differences of counts '
             'in a selection of open data sources for a short list of DOIs '
             'and to make aware that it is worth investigating '
             'the characteristics of a data source before using it to carry out an analysis.')


def generate_tab_direct(df0, count, tab):
    with tab: 
        st.header(f'{count.capitalize()} count')
        cols = st.columns([4, 1], gap='large')
        viz.write_count_table(df0, databases, count_category=count, cols=cols)
        viz.plot_rel_count_plotly(df0, databases, count)



def generate_docs():
    st.header('Contact us')
    st.markdown('If you have any questions or feedback, please contact us through the [project page](https://eth-library.github.io/tobi/).')
    st.header('Documentation on the computation of the counts')
    st.subheader('Crossref')
    st.write('The *Citations count* corresponds to the *is-referenced-by-count* field.')
    st.write('The *References count* corresponds to the *references-count* field.')
    st.write('The *Authors count* is computed by getting the length of the *author* field.')
    st.subheader('OpenAlex')
    st.write('The *Citations count* corresponds to the *cited_by_count* field.')
    st.write('The *References count* is computed by getting the length of the *referenced_work* field.')
    st.write('The *Authors count* is computed by getting the length of the *authorships* field.')
    st.subheader('OpenCitations')
    st.write('The *Citations count* corresponds to the *citation-count* in the OpenCitations Index')
    st.write('The *References count* corresponds to the *reference-count* in the OpenCitations Index')
    st.write('The *Authors count* is computed by adding 1 to the number of semicolons (";") in the *author* field in OpenCitations Meta')
    st.subheader('Semantic Scholar')
    st.write('The *Citations count* corresponds to the *citationCount* field.')
    st.write('The *References count* corresponds to the *referenceCount* field.')
    st.write('The *Authors count* is computing by getting the length of the *authors* field.')


def csv_download_button():
    st.download_button("Click to Download data (csv)", csv, "counts.csv")

df_swissuniversities_members = pd.DataFrame([
    ['École Polytechnique Fédérale de Lausanne', 'https://openalex.org/I5124864'],
    ['ETH Zurich', 'https://openalex.org/I35440088'],
    ['University of Basel', 'https://openalex.org/I1850255'],
    ['University of Bern', 'https://openalex.org/I118564535'],
    ['University of Fribourg', 'https://openalex.org/I154338468'],
    ['University of Geneva', 'https://openalex.org/I114457229'],
    ['University of Lausanne', 'https://openalex.org/I97565354'],
    ['University of Lucerne', 'https://openalex.org/I161941770'],
    ['University of Neuchâtel', 'https://openalex.org/I57825437'],
    ['University of St. Gallen', 'https://openalex.org/I202963720'],
    ['Università della Svizzera italiana', 'https://openalex.org/I57201433'],
    ['University of Zurich', 'https://openalex.org/I202697423'],
    ['Bern University of Applied Sciences', 'https://openalex.org/I130692619'],
    ['University of Applied Sciences of the Grisons', 'https://openalex.org/I4210120439'],
    ['University of Applied Sciences and Arts Northwestern Switzerland', 'https://openalex.org/I2972652528'],
    ['University of Applied Sciences and Arts Western Switzerland', 'https://openalex.org/I173439891'],
    ['Lucerne University of Applied Sciences and Arts', 'https://openalex.org/I81007117'],
    ['Kalaidos University of Applied Sciences', 'https://openalex.org/I3132934759'],
    ['Ostschweizer Fachhochschule OST', 'https://openalex.org/I4210129390'],
    ['University of Applied Sciences and Arts of Southern Switzerland', 'https://openalex.org/I15196421'],
    ['Zurich University of the Arts', 'https://openalex.org/I64152125'],
    ['ZHAW Zurich University of Applied Sciences', 'https://openalex.org/I200744771'],
    ['Haute École Pédagogique BEJUNE', 'https://openalex.org/I4210101117'],
    ['Haute École Pédagogique du Canton de Vaud', 'https://openalex.org/I4210106586'],
    ['Pädagogische Hochschule Wallis', 'https://openalex.org/I4210141884'],
    ['Haute École Pédagogique Fribourg', 'https://openalex.org/I4210137605'],
    ['NMS Berne', 'https://openalex.org/I4210143584'],
    ['University of Teacher Education in Special Needs', 'https://openalex.org/I4210086400'],
    ['Pädagogische Hochschule Graubünden', 'https://openalex.org/I4210117930'],
    ['Pädagogische Hochschule Bern', 'https://openalex.org/I4210160491'],
    ['University of Teacher Education Lucerne', 'https://openalex.org/I4210112078'],
    ['St.Gallen University of Teacher Education', 'https://openalex.org/I4210158813'],
    ['Pädagogische Hochschule Schaffhausen', 'https://openalex.org/I4210139224'],
    ['Schwyz University of Teacher Education', 'https://openalex.org/I4210095907'],
    ['Thurgau University of Teacher Education', 'https://openalex.org/I4210138261'],
    ['Zurich University of Teacher Education', 'https://openalex.org/I4210111868'],
    ['University of Teacher Education Zug', 'https://openalex.org/I4210146564'],
    ['Swiss Federal University for Vocational Education and Training SFUVET', 'https://openalex.org/I4210097053']],
    columns=['institution_name', 'institution_id'])


########## Page config

st.set_page_config(layout="wide", 
                   page_title="Track your open scholarly metadata")


############ Streamlit App Sidebar
css = '''
<style>
    [data-testid="stSidebar"]{
        min-width: 450px;
        max-width: 650px;
    }
<style>
'''
st.markdown(css, unsafe_allow_html=True)

example = 0
dois = ''
sample = []

with st.sidebar:
    st.title('Input')
    with st.expander('DOIs', expanded=True):
        sample_size = 10  # should be <= 20
        input_method = st.radio('Select a method',
                                ('Manually',
                                f'Random sample of {sample_size} DOIs from OpenAlex',))
        if input_method == 'Manually':
            example = st.text_area("Enter your DOIs (one DOI per line)", '\n'.join(map(str, sample)), height=300)
            # if example:
            example = str.splitlines(example)
        else:  # input_method == f'Random sample of {sample_size} DOIs from OpenAlex':
            input_institution = st.radio('Select an institution',
                                         ('Swissuniversities institution', 'OpenAlex institution ID'))
            if input_institution == 'Swissuniversities institution':
                institution_name = st.selectbox(
                    'Select a (random) swissuniversities member institution',
                    ['Random swissuniversities institution', ''] + df_swissuniversities_members[
                        'institution_name'].tolist())
                if institution_name == "Random swissuniversities institution":
                    institution_id, institution_name = get_random_institution(df_swissuniversities_members)
                elif institution_name:
                    institution_id = \
                        df_swissuniversities_members[df_swissuniversities_members['institution_name'] == institution_name][
                            'institution_id'].iloc[0]
                # if st.button(
                #        f'Get a random sample from OpenAlex'
                # ):
                # with st.spinner(text=f"Loading OpenAlex sample..."):
                if institution_name:
                    example = api.get_openalex_sample(sample_size, institution_id)
                    input_method += f' with an author affiliation \n to {institution_name}'
            elif input_institution == 'OpenAlex institution ID':
                institution_id = st.text_input("Enter the OpenAlex id of your institution")
                if not institution_id:
                    st.write('You can find OpenAlex id using this link: https://explore.openalex.org/')
                    # st.stop()
                else:
                    url = f"https://api.openalex.org/institutions/{institution_id}"
                    r = requests.get(url)
                    if r.status_code == 404:
                        random_button = st.warning(
                            'Please enter a valid institution OpenAlex id (https://explore.openalex.org/).')
                        st.stop()
                    else:
                        institution_id = r.json()['id']
                        institution_name = r.json()['display_name']
                        input_method += f' with an author affiliation \n to {institution_name}'
                    with st.spinner(text=f"Loading sample..."):
                        example = api.get_openalex_sample(sample_size, institution_id)

        if example != 0:
            dois = format_doi_list(example)

            if input_method != 'Manually':
                if st.button('Get new random sample of DOIs'):
                    st.cache_data.clear()
            st.write(f'Input: {input_method}')
            st.text(f'Number of unique DOIs: {len(dois)}')

    with st.expander('Data sources', expanded=True):
        db_selection = st.multiselect(
            "Select open data sources",
            ['Crossref', 'OpenAlex', 'OpenCitations', 'Semantic Scholar'],
            default=['Crossref', 'OpenAlex', 'OpenCitations', 'Semantic Scholar'])

    with st.expander('Polite pool settings'):
        my_email_address = st.text_input("Email address for Crossref and OpenAlex polite pool (optional)", '')
        opencitations_access_token = st.text_input("OpenCitations access token (optional)", '')
        semanticscholar_api_key = st.text_input("Semantic Scholar API key (optional)", '')

    st.title('Load data')
    if st.button('Click to load data'):
        load, df = api.load_data(dois, db_selection,
                                 my_email_address,
                                 opencitations_access_token,
                                 semanticscholar_api_key)
        if load == 'Success':
            df, databases, df_pivoted = prepare_df(df, dois)
            if not df.empty:
                st.session_state['dois'] = dois
                st.session_state['df'] = df
                st.session_state['databases'] = databases
                st.session_state['df_pivoted'] = df_pivoted


############################ Main App

##### App Header 


st.title('Track your open scholarly metadata')
# if 'df' in st.session_state:
#     st.download_button("Download data (csv)", csv, "counts.csv")

st.write('''Wondering how your research is being represented in open bibliometric data sources? '''
         '''Enter your DOIs in the sidebar and select some open data sources for an easy comparison. '''
         '''Alternatively, pick an institution from the list and '''
         '''inspect a random sample of 10 DOIs affiliated with it.''')


###### Check if data loaded 

if 'df' not in st.session_state:
    st.warning('No data found. Define your input in the sidebar and click on *Click to load data*.')
    st.stop()
else:
    dois = st.session_state['dois']
    df = st.session_state['df']
    databases = st.session_state['databases']
    df_pivoted = st.session_state['df_pivoted']
    # Data export
    csv = df_pivoted.to_csv(index=False).encode('utf-8')

# Statistics
df_pivoted['median'] = df_pivoted[databases].median(axis=1, skipna=True)
df_pivoted['mean'] = df_pivoted[databases].mean(axis=1, skipna=True)
df_pivoted['sd'] = df_pivoted[databases].std(axis=1, skipna=True)
df_pivoted['CV'] = df_pivoted['sd']/df_pivoted['mean']


##### Create tabs if data loaded 


## define the tab content 
tab_dict = {
    "Citations count": lambda x: generate_tab(df_pivoted, 'citations'), 
    "References count": lambda x: generate_tab(df_pivoted, 'references'),
    "Authors count": lambda x: generate_tab(df_pivoted, 'authors'),
    "Documentation": lambda x: generate_docs(), 
    "Download data": lambda x: csv_download_button(),
}

tabs = st.tabs(list(tab_dict.keys()))

for t, kv in zip(tabs, tab_dict.items()):
    with t: 
        kv[1](None)  # None in order to evaluate the lambda function


#### Footer 

footer = """<style>
.footer {
position: fixed;
left: 0;
bottom: 0;
width: 100%;
background-color: white;
color: black;
text-align: center;
}
</style>
<div class="footer">
<p>(c) by ETH Library, within the project <a href="https://eth-library.github.io/tobi/">TOBI</a></p>
</div>
"""
st.markdown(footer, unsafe_allow_html=True)
