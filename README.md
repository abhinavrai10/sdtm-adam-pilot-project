# DISCLAIMER AND LIABILITY

This website provides clinical test data for informational purposes and the convenience of the public. CDISC does not control or guarantee the currency, accuracy, relevance, or completeness of the data. The data has been analyzed, cleansed, and aggregated where appropriate to facilitate use and discussion in research. By downloading / using this data, you agree to the Terms of use.

## Terms of Use

You shall not (and will not allow or assist any third party to), under any circumstances: (i) mislead, confuse, or cause misapprehension or confusion among users of the data as to the features, functionality, origin, capabilities, or other aspects of the data, (b) disassemble, reverse engineer, decompile, modify, or alter any part of the Data; (c) use the Data in any manner or for any purpose that violates any law or regulation, (d) use the Data in order to compete with CDISC, (e) sublicense or distribute the data for a fee, or (f) sublicense or distribute the data without an attribution back to CDISC.

UNLESS REQUIRED BY APPLICABLE LAW, ACCESS AND USE OF THE DATA IS PROVIDED BY CDISC AND ITS CONSTITUENT PARTS (INCLUDING, BUT NOT LIMITED TO THE CDISC BOARD OF DIRECTORS, CDISC EMPLOYEES, AND CDISC MEMBERS, PARTICIPANTS, CONTRACTORS, AND REPRESENTATIVES) "AS IS" AND WITHOUT ANY WARRANTIES WHATSOEVER, WHETHER EXPRESS, IMPLIED, STATUTORY, OR OTHERWISE, AND CDISC AND ITS CONSTITUENT PARTS (INCLUDING, BUT NOT LIMITED TO THE CDISC BOARD OF DIRECTORS, CDISC EMPLOYEES, AND CDISC MEMBERS, PARTICIPANTS, CONTRACTORS, AND REPRESENTATIVES) EXPRESSLY DISCLAIM ANY WARRANTY OF  MERCHANTABILITY, TITLE, NONINFRINGEMENT, FITNESS FOR A PARTICULAR OR INTENDED PURPOSE, OR ANY OTHER WARRANTY OTHERWISE ARISING OUT OF THIS LETTER AGREEMENT, INCLUDING ACCESS OR USE OF THE DATA. You are solely responsible for determining the appropriateness of accessing and/or using the data and assume any risks associated with your access and/or use.

IN NO EVENT AND UNDER NO LEGAL THEORY, WHETHER IN TORT (INCLUDING NEGLIGENCE), CONTRACT, OR OTHERWISE, UNLESS REQUIRED BY APPLICABLE LAW (SUCH AS DELIBERATE AND GROSSLY NEGLIGENT ACTS) OR AGREED TO IN WRITING, SHALL CDISC, ANY OF CDISC’S CONSTITUENT PARTS (INCLUDING, BUT NOT LIMITED TO THE CDISC BOARD OF DIRECTORS, THE CDISC EMPLOYEES, OR CDISC MEMBERS, PARTICIPANTS, CONTRACTORS, OR REPRESENTATIVES) BE LIABLE FOR DAMAGES, INCLUDING ANY DIRECT, INDIRECT, SPECIAL, EXEMPLARY, INCIDENTAL, OR CONSEQUENTIAL DAMAGES OF ANY CHARACTER ARISING IN ANY WAY AS A RESULT OF THIS LETTER AGREEMENT OR OUT OF THE USE OR INABILITY TO USE THE DATA (INCLUDING DAMAGES FOR LOSS OF GOODWILL, LOSS OF PROFITS, LOSS OF USE, OR BUSINESS INTERRUPTION), EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGES.  THIS LIMITATION OF DAMAGES AND CLAIMS IS INTENDED TO APPLY TO ALL CLAIMS WITHOUT REGARD TO WHICH OTHER PROVISIONS OF THIS LETTER AGREEMENT HAVE BEEN BREACHED OR PROVEN INEFFECTIVE.

---

# CDISC Oncology ETL: Transformations for DM, AE, and LB Datasets

### Imports


```python
import pandas as pd
import pyreadstat
import os
```

# DM.xpt data

### Load raw DM.xpt


```python
def load_dm_data():
    data_path = r'updated-pilot-submission-package\900172\m5\datasets\cdiscpilot01\tabulations\sdtm'
    dm_df, dm_meta = pyreadstat.read_xport(os.path.join(data_path, 'dm.xpt'))
    return dm_df

dm_df = load_dm_data()
dm_df.head(5)

dm_df.columns
```




    Index(['STUDYID', 'DOMAIN', 'USUBJID', 'SUBJID', 'RFSTDTC', 'RFENDTC',
           'RFXSTDTC', 'RFXENDTC', 'RFICDTC', 'RFPENDTC', 'DTHDTC', 'DTHFL',
           'SITEID', 'AGE', 'AGEU', 'SEX', 'RACE', 'ETHNIC', 'ARMCD', 'ARM',
           'ACTARMCD', 'ACTARM', 'COUNTRY', 'DMDTC', 'DMDY'],
          dtype='object')



### 1. Missing Value Imputation


```python
def impute_missing(df):
    df = df.copy()
    df['RACE'] = df['RACE'].fillna('Unknown')
    df['ETHNIC'] = df['ETHNIC'].fillna('Unknown')
    df['COUNTRY'] = df['COUNTRY'].fillna(df['COUNTRY'].mode()[0] if not df['COUNTRY'].mode().empty else 'Unknown')
    df['DMDY'] = df['DMDY'].fillna(df['DMDY'].median())
    df.dropna(subset=['USUBJID'], inplace=True)
    return df

impute_missing_df = impute_missing(dm_df)
impute_missing_df.head(5)
    
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>STUDYID</th>
      <th>DOMAIN</th>
      <th>USUBJID</th>
      <th>SUBJID</th>
      <th>RFSTDTC</th>
      <th>RFENDTC</th>
      <th>RFXSTDTC</th>
      <th>RFXENDTC</th>
      <th>RFICDTC</th>
      <th>RFPENDTC</th>
      <th>...</th>
      <th>SEX</th>
      <th>RACE</th>
      <th>ETHNIC</th>
      <th>ARMCD</th>
      <th>ARM</th>
      <th>ACTARMCD</th>
      <th>ACTARM</th>
      <th>COUNTRY</th>
      <th>DMDTC</th>
      <th>DMDY</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>CDISCPILOT01</td>
      <td>DM</td>
      <td>01-701-1015</td>
      <td>1015</td>
      <td>2014-01-02</td>
      <td>2014-07-02</td>
      <td>2014-01-02</td>
      <td>2014-07-02</td>
      <td></td>
      <td>2014-07-02T11:45</td>
      <td>...</td>
      <td>F</td>
      <td>WHITE</td>
      <td>HISPANIC OR LATINO</td>
      <td>Pbo</td>
      <td>Placebo</td>
      <td>Pbo</td>
      <td>Placebo</td>
      <td>USA</td>
      <td>2013-12-26</td>
      <td>-7.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>CDISCPILOT01</td>
      <td>DM</td>
      <td>01-701-1023</td>
      <td>1023</td>
      <td>2012-08-05</td>
      <td>2012-09-02</td>
      <td>2012-08-05</td>
      <td>2012-09-01</td>
      <td></td>
      <td>2013-02-18</td>
      <td>...</td>
      <td>M</td>
      <td>WHITE</td>
      <td>HISPANIC OR LATINO</td>
      <td>Pbo</td>
      <td>Placebo</td>
      <td>Pbo</td>
      <td>Placebo</td>
      <td>USA</td>
      <td>2012-07-22</td>
      <td>-14.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>CDISCPILOT01</td>
      <td>DM</td>
      <td>01-701-1028</td>
      <td>1028</td>
      <td>2013-07-19</td>
      <td>2014-01-14</td>
      <td>2013-07-19</td>
      <td>2014-01-14</td>
      <td></td>
      <td>2014-01-14T11:10</td>
      <td>...</td>
      <td>M</td>
      <td>WHITE</td>
      <td>NOT HISPANIC OR LATINO</td>
      <td>Xan_Hi</td>
      <td>Xanomeline High Dose</td>
      <td>Xan_Hi</td>
      <td>Xanomeline High Dose</td>
      <td>USA</td>
      <td>2013-07-11</td>
      <td>-8.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>CDISCPILOT01</td>
      <td>DM</td>
      <td>01-701-1033</td>
      <td>1033</td>
      <td>2014-03-18</td>
      <td>2014-04-14</td>
      <td>2014-03-18</td>
      <td>2014-03-31</td>
      <td></td>
      <td>2014-09-15</td>
      <td>...</td>
      <td>M</td>
      <td>WHITE</td>
      <td>NOT HISPANIC OR LATINO</td>
      <td>Xan_Lo</td>
      <td>Xanomeline Low Dose</td>
      <td>Xan_Lo</td>
      <td>Xanomeline Low Dose</td>
      <td>USA</td>
      <td>2014-03-10</td>
      <td>-8.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>CDISCPILOT01</td>
      <td>DM</td>
      <td>01-701-1034</td>
      <td>1034</td>
      <td>2014-07-01</td>
      <td>2014-12-30</td>
      <td>2014-07-01</td>
      <td>2014-12-30</td>
      <td></td>
      <td>2014-12-30T09:50</td>
      <td>...</td>
      <td>F</td>
      <td>WHITE</td>
      <td>NOT HISPANIC OR LATINO</td>
      <td>Xan_Hi</td>
      <td>Xanomeline High Dose</td>
      <td>Xan_Hi</td>
      <td>Xanomeline High Dose</td>
      <td>USA</td>
      <td>2014-06-24</td>
      <td>-7.0</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 25 columns</p>
</div>



### 2. Date Standardization


```python
def standardize_dates(df):
    df = df.copy()
    date_cols = ['RFSTDTC', 'RFENDTC', 'RFXSTDTC', 'RFXENDTC', 'RFICDTC', 'RFPENDTC', 'DTHDTC', 'DMDTC']
    for col in date_cols:
        if col in dm_df.columns:
           df[col] = pd.to_datetime(dm_df[col], errors='coerce')
    return df

standardize_df = standardize_dates(impute_missing_df)
standardize_df.head(5)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>STUDYID</th>
      <th>DOMAIN</th>
      <th>USUBJID</th>
      <th>SUBJID</th>
      <th>RFSTDTC</th>
      <th>RFENDTC</th>
      <th>RFXSTDTC</th>
      <th>RFXENDTC</th>
      <th>RFICDTC</th>
      <th>RFPENDTC</th>
      <th>...</th>
      <th>SEX</th>
      <th>RACE</th>
      <th>ETHNIC</th>
      <th>ARMCD</th>
      <th>ARM</th>
      <th>ACTARMCD</th>
      <th>ACTARM</th>
      <th>COUNTRY</th>
      <th>DMDTC</th>
      <th>DMDY</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>CDISCPILOT01</td>
      <td>DM</td>
      <td>01-701-1015</td>
      <td>1015</td>
      <td>2014-01-02</td>
      <td>2014-07-02</td>
      <td>2014-01-02</td>
      <td>2014-07-02</td>
      <td>NaT</td>
      <td>2014-07-02 11:45:00</td>
      <td>...</td>
      <td>F</td>
      <td>WHITE</td>
      <td>HISPANIC OR LATINO</td>
      <td>Pbo</td>
      <td>Placebo</td>
      <td>Pbo</td>
      <td>Placebo</td>
      <td>USA</td>
      <td>2013-12-26</td>
      <td>-7.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>CDISCPILOT01</td>
      <td>DM</td>
      <td>01-701-1023</td>
      <td>1023</td>
      <td>2012-08-05</td>
      <td>2012-09-02</td>
      <td>2012-08-05</td>
      <td>2012-09-01</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>...</td>
      <td>M</td>
      <td>WHITE</td>
      <td>HISPANIC OR LATINO</td>
      <td>Pbo</td>
      <td>Placebo</td>
      <td>Pbo</td>
      <td>Placebo</td>
      <td>USA</td>
      <td>2012-07-22</td>
      <td>-14.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>CDISCPILOT01</td>
      <td>DM</td>
      <td>01-701-1028</td>
      <td>1028</td>
      <td>2013-07-19</td>
      <td>2014-01-14</td>
      <td>2013-07-19</td>
      <td>2014-01-14</td>
      <td>NaT</td>
      <td>2014-01-14 11:10:00</td>
      <td>...</td>
      <td>M</td>
      <td>WHITE</td>
      <td>NOT HISPANIC OR LATINO</td>
      <td>Xan_Hi</td>
      <td>Xanomeline High Dose</td>
      <td>Xan_Hi</td>
      <td>Xanomeline High Dose</td>
      <td>USA</td>
      <td>2013-07-11</td>
      <td>-8.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>CDISCPILOT01</td>
      <td>DM</td>
      <td>01-701-1033</td>
      <td>1033</td>
      <td>2014-03-18</td>
      <td>2014-04-14</td>
      <td>2014-03-18</td>
      <td>2014-03-31</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>...</td>
      <td>M</td>
      <td>WHITE</td>
      <td>NOT HISPANIC OR LATINO</td>
      <td>Xan_Lo</td>
      <td>Xanomeline Low Dose</td>
      <td>Xan_Lo</td>
      <td>Xanomeline Low Dose</td>
      <td>USA</td>
      <td>2014-03-10</td>
      <td>-8.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>CDISCPILOT01</td>
      <td>DM</td>
      <td>01-701-1034</td>
      <td>1034</td>
      <td>2014-07-01</td>
      <td>2014-12-30</td>
      <td>2014-07-01</td>
      <td>2014-12-30</td>
      <td>NaT</td>
      <td>2014-12-30 09:50:00</td>
      <td>...</td>
      <td>F</td>
      <td>WHITE</td>
      <td>NOT HISPANIC OR LATINO</td>
      <td>Xan_Hi</td>
      <td>Xanomeline High Dose</td>
      <td>Xan_Hi</td>
      <td>Xanomeline High Dose</td>
      <td>USA</td>
      <td>2014-06-24</td>
      <td>-7.0</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 25 columns</p>
</div>



### 3. Data Type Enforcement (per CDISC specs)


```python
def enforce_data_types(df):
    df = df.copy()
    df['AGE'] = pd.to_numeric(df['AGE'], errors='coerce').astype('Int64')
    df['AGEU'] = df['AGEU'].astype('category')
    df['SEX'] = df['SEX'].astype('category')
    df['RACE'] = df['RACE'].astype('category')
    df['ETHNIC'] = df['ETHNIC'].astype('category')
    df['STUDYID'] = df['STUDYID'].astype(str)
    df['DMDY'] = pd.to_numeric(df['DMDY'], errors='coerce').astype('Int64')
    df['DTHFL'] = df['DTHFL'].astype('category')
    return df

enforce_data_df = enforce_data_types(standardize_df)
enforce_data_df.head(5)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>STUDYID</th>
      <th>DOMAIN</th>
      <th>USUBJID</th>
      <th>SUBJID</th>
      <th>RFSTDTC</th>
      <th>RFENDTC</th>
      <th>RFXSTDTC</th>
      <th>RFXENDTC</th>
      <th>RFICDTC</th>
      <th>RFPENDTC</th>
      <th>...</th>
      <th>SEX</th>
      <th>RACE</th>
      <th>ETHNIC</th>
      <th>ARMCD</th>
      <th>ARM</th>
      <th>ACTARMCD</th>
      <th>ACTARM</th>
      <th>COUNTRY</th>
      <th>DMDTC</th>
      <th>DMDY</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>CDISCPILOT01</td>
      <td>DM</td>
      <td>01-701-1015</td>
      <td>1015</td>
      <td>2014-01-02</td>
      <td>2014-07-02</td>
      <td>2014-01-02</td>
      <td>2014-07-02</td>
      <td>NaT</td>
      <td>2014-07-02 11:45:00</td>
      <td>...</td>
      <td>F</td>
      <td>WHITE</td>
      <td>HISPANIC OR LATINO</td>
      <td>Pbo</td>
      <td>Placebo</td>
      <td>Pbo</td>
      <td>Placebo</td>
      <td>USA</td>
      <td>2013-12-26</td>
      <td>-7</td>
    </tr>
    <tr>
      <th>1</th>
      <td>CDISCPILOT01</td>
      <td>DM</td>
      <td>01-701-1023</td>
      <td>1023</td>
      <td>2012-08-05</td>
      <td>2012-09-02</td>
      <td>2012-08-05</td>
      <td>2012-09-01</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>...</td>
      <td>M</td>
      <td>WHITE</td>
      <td>HISPANIC OR LATINO</td>
      <td>Pbo</td>
      <td>Placebo</td>
      <td>Pbo</td>
      <td>Placebo</td>
      <td>USA</td>
      <td>2012-07-22</td>
      <td>-14</td>
    </tr>
    <tr>
      <th>2</th>
      <td>CDISCPILOT01</td>
      <td>DM</td>
      <td>01-701-1028</td>
      <td>1028</td>
      <td>2013-07-19</td>
      <td>2014-01-14</td>
      <td>2013-07-19</td>
      <td>2014-01-14</td>
      <td>NaT</td>
      <td>2014-01-14 11:10:00</td>
      <td>...</td>
      <td>M</td>
      <td>WHITE</td>
      <td>NOT HISPANIC OR LATINO</td>
      <td>Xan_Hi</td>
      <td>Xanomeline High Dose</td>
      <td>Xan_Hi</td>
      <td>Xanomeline High Dose</td>
      <td>USA</td>
      <td>2013-07-11</td>
      <td>-8</td>
    </tr>
    <tr>
      <th>3</th>
      <td>CDISCPILOT01</td>
      <td>DM</td>
      <td>01-701-1033</td>
      <td>1033</td>
      <td>2014-03-18</td>
      <td>2014-04-14</td>
      <td>2014-03-18</td>
      <td>2014-03-31</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>...</td>
      <td>M</td>
      <td>WHITE</td>
      <td>NOT HISPANIC OR LATINO</td>
      <td>Xan_Lo</td>
      <td>Xanomeline Low Dose</td>
      <td>Xan_Lo</td>
      <td>Xanomeline Low Dose</td>
      <td>USA</td>
      <td>2014-03-10</td>
      <td>-8</td>
    </tr>
    <tr>
      <th>4</th>
      <td>CDISCPILOT01</td>
      <td>DM</td>
      <td>01-701-1034</td>
      <td>1034</td>
      <td>2014-07-01</td>
      <td>2014-12-30</td>
      <td>2014-07-01</td>
      <td>2014-12-30</td>
      <td>NaT</td>
      <td>2014-12-30 09:50:00</td>
      <td>...</td>
      <td>F</td>
      <td>WHITE</td>
      <td>NOT HISPANIC OR LATINO</td>
      <td>Xan_Hi</td>
      <td>Xanomeline High Dose</td>
      <td>Xan_Hi</td>
      <td>Xanomeline High Dose</td>
      <td>USA</td>
      <td>2014-06-24</td>
      <td>-7</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 25 columns</p>
</div>



### 4. Derived Variables (insights-focused for oncology)


```python
def derive_variables(df):
    df = df.copy()
    
    def age_group(age):
        if pd.isna(age):
            return 'Unknown'
        elif age < 18:
            return 'Pediatric'
        elif age <= 65:
            return 'Adult'
        else:
            return 'Senior'
    df['AGE_GROUP'] = df['AGE'].apply(age_group)
    
    df['STUDY_DURATION'] = df['DMDY'].fillna(0)
    if 'RFXENDTC' in df.columns and 'RFXSTDTC' in df.columns:
        df['STUDY_DURATION'] = df['STUDY_DURATION'].fillna((df['RFXENDTC'] - df['RFXSTDTC']).dt.days)
    
    df['ARM_TYPE'] = df['ARM'].str.lower().str.contains('placebo|control', na=False).map({True: 'Control', False: 'Treatment'}).fillna('Unknown')
    return df

derive_variables_df = derive_variables(enforce_data_df)
derive_variables_df.head(5)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>STUDYID</th>
      <th>DOMAIN</th>
      <th>USUBJID</th>
      <th>SUBJID</th>
      <th>RFSTDTC</th>
      <th>RFENDTC</th>
      <th>RFXSTDTC</th>
      <th>RFXENDTC</th>
      <th>RFICDTC</th>
      <th>RFPENDTC</th>
      <th>...</th>
      <th>ARMCD</th>
      <th>ARM</th>
      <th>ACTARMCD</th>
      <th>ACTARM</th>
      <th>COUNTRY</th>
      <th>DMDTC</th>
      <th>DMDY</th>
      <th>AGE_GROUP</th>
      <th>STUDY_DURATION</th>
      <th>ARM_TYPE</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>CDISCPILOT01</td>
      <td>DM</td>
      <td>01-701-1015</td>
      <td>1015</td>
      <td>2014-01-02</td>
      <td>2014-07-02</td>
      <td>2014-01-02</td>
      <td>2014-07-02</td>
      <td>NaT</td>
      <td>2014-07-02 11:45:00</td>
      <td>...</td>
      <td>Pbo</td>
      <td>Placebo</td>
      <td>Pbo</td>
      <td>Placebo</td>
      <td>USA</td>
      <td>2013-12-26</td>
      <td>-7</td>
      <td>Adult</td>
      <td>-7</td>
      <td>Control</td>
    </tr>
    <tr>
      <th>1</th>
      <td>CDISCPILOT01</td>
      <td>DM</td>
      <td>01-701-1023</td>
      <td>1023</td>
      <td>2012-08-05</td>
      <td>2012-09-02</td>
      <td>2012-08-05</td>
      <td>2012-09-01</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>...</td>
      <td>Pbo</td>
      <td>Placebo</td>
      <td>Pbo</td>
      <td>Placebo</td>
      <td>USA</td>
      <td>2012-07-22</td>
      <td>-14</td>
      <td>Adult</td>
      <td>-14</td>
      <td>Control</td>
    </tr>
    <tr>
      <th>2</th>
      <td>CDISCPILOT01</td>
      <td>DM</td>
      <td>01-701-1028</td>
      <td>1028</td>
      <td>2013-07-19</td>
      <td>2014-01-14</td>
      <td>2013-07-19</td>
      <td>2014-01-14</td>
      <td>NaT</td>
      <td>2014-01-14 11:10:00</td>
      <td>...</td>
      <td>Xan_Hi</td>
      <td>Xanomeline High Dose</td>
      <td>Xan_Hi</td>
      <td>Xanomeline High Dose</td>
      <td>USA</td>
      <td>2013-07-11</td>
      <td>-8</td>
      <td>Senior</td>
      <td>-8</td>
      <td>Treatment</td>
    </tr>
    <tr>
      <th>3</th>
      <td>CDISCPILOT01</td>
      <td>DM</td>
      <td>01-701-1033</td>
      <td>1033</td>
      <td>2014-03-18</td>
      <td>2014-04-14</td>
      <td>2014-03-18</td>
      <td>2014-03-31</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>...</td>
      <td>Xan_Lo</td>
      <td>Xanomeline Low Dose</td>
      <td>Xan_Lo</td>
      <td>Xanomeline Low Dose</td>
      <td>USA</td>
      <td>2014-03-10</td>
      <td>-8</td>
      <td>Senior</td>
      <td>-8</td>
      <td>Treatment</td>
    </tr>
    <tr>
      <th>4</th>
      <td>CDISCPILOT01</td>
      <td>DM</td>
      <td>01-701-1034</td>
      <td>1034</td>
      <td>2014-07-01</td>
      <td>2014-12-30</td>
      <td>2014-07-01</td>
      <td>2014-12-30</td>
      <td>NaT</td>
      <td>2014-12-30 09:50:00</td>
      <td>...</td>
      <td>Xan_Hi</td>
      <td>Xanomeline High Dose</td>
      <td>Xan_Hi</td>
      <td>Xanomeline High Dose</td>
      <td>USA</td>
      <td>2014-06-24</td>
      <td>-7</td>
      <td>Senior</td>
      <td>-7</td>
      <td>Treatment</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 28 columns</p>
</div>



### 5. Outlier Detection/Cleaning


```python
def clean_outliers(df):
    df = df.copy()
    print("Before filters:", df.shape)
    
    # AGE: Keep if not NaN and in range
    df = df[(df['AGE'].notna()) & (df['AGE'] >= 0) & (df['AGE'] <= 120)]
    print("After AGE filter:", df.shape)
    
    df = df.drop_duplicates(subset=['USUBJID'])
    print("After duplicates:", df.shape)
    return df
    
clean_outliers_df = clean_outliers(derive_variables_df)
clean_outliers_df.head(5)
```

    Before filters: (306, 28)
    After AGE filter: (306, 28)
    After duplicates: (306, 28)
    




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>STUDYID</th>
      <th>DOMAIN</th>
      <th>USUBJID</th>
      <th>SUBJID</th>
      <th>RFSTDTC</th>
      <th>RFENDTC</th>
      <th>RFXSTDTC</th>
      <th>RFXENDTC</th>
      <th>RFICDTC</th>
      <th>RFPENDTC</th>
      <th>...</th>
      <th>ARMCD</th>
      <th>ARM</th>
      <th>ACTARMCD</th>
      <th>ACTARM</th>
      <th>COUNTRY</th>
      <th>DMDTC</th>
      <th>DMDY</th>
      <th>AGE_GROUP</th>
      <th>STUDY_DURATION</th>
      <th>ARM_TYPE</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>CDISCPILOT01</td>
      <td>DM</td>
      <td>01-701-1015</td>
      <td>1015</td>
      <td>2014-01-02</td>
      <td>2014-07-02</td>
      <td>2014-01-02</td>
      <td>2014-07-02</td>
      <td>NaT</td>
      <td>2014-07-02 11:45:00</td>
      <td>...</td>
      <td>Pbo</td>
      <td>Placebo</td>
      <td>Pbo</td>
      <td>Placebo</td>
      <td>USA</td>
      <td>2013-12-26</td>
      <td>-7</td>
      <td>Adult</td>
      <td>-7</td>
      <td>Control</td>
    </tr>
    <tr>
      <th>1</th>
      <td>CDISCPILOT01</td>
      <td>DM</td>
      <td>01-701-1023</td>
      <td>1023</td>
      <td>2012-08-05</td>
      <td>2012-09-02</td>
      <td>2012-08-05</td>
      <td>2012-09-01</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>...</td>
      <td>Pbo</td>
      <td>Placebo</td>
      <td>Pbo</td>
      <td>Placebo</td>
      <td>USA</td>
      <td>2012-07-22</td>
      <td>-14</td>
      <td>Adult</td>
      <td>-14</td>
      <td>Control</td>
    </tr>
    <tr>
      <th>2</th>
      <td>CDISCPILOT01</td>
      <td>DM</td>
      <td>01-701-1028</td>
      <td>1028</td>
      <td>2013-07-19</td>
      <td>2014-01-14</td>
      <td>2013-07-19</td>
      <td>2014-01-14</td>
      <td>NaT</td>
      <td>2014-01-14 11:10:00</td>
      <td>...</td>
      <td>Xan_Hi</td>
      <td>Xanomeline High Dose</td>
      <td>Xan_Hi</td>
      <td>Xanomeline High Dose</td>
      <td>USA</td>
      <td>2013-07-11</td>
      <td>-8</td>
      <td>Senior</td>
      <td>-8</td>
      <td>Treatment</td>
    </tr>
    <tr>
      <th>3</th>
      <td>CDISCPILOT01</td>
      <td>DM</td>
      <td>01-701-1033</td>
      <td>1033</td>
      <td>2014-03-18</td>
      <td>2014-04-14</td>
      <td>2014-03-18</td>
      <td>2014-03-31</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>...</td>
      <td>Xan_Lo</td>
      <td>Xanomeline Low Dose</td>
      <td>Xan_Lo</td>
      <td>Xanomeline Low Dose</td>
      <td>USA</td>
      <td>2014-03-10</td>
      <td>-8</td>
      <td>Senior</td>
      <td>-8</td>
      <td>Treatment</td>
    </tr>
    <tr>
      <th>4</th>
      <td>CDISCPILOT01</td>
      <td>DM</td>
      <td>01-701-1034</td>
      <td>1034</td>
      <td>2014-07-01</td>
      <td>2014-12-30</td>
      <td>2014-07-01</td>
      <td>2014-12-30</td>
      <td>NaT</td>
      <td>2014-12-30 09:50:00</td>
      <td>...</td>
      <td>Xan_Hi</td>
      <td>Xanomeline High Dose</td>
      <td>Xan_Hi</td>
      <td>Xanomeline High Dose</td>
      <td>USA</td>
      <td>2014-06-24</td>
      <td>-7</td>
      <td>Senior</td>
      <td>-7</td>
      <td>Treatment</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 28 columns</p>
</div>



### 6. Consistency Checks (CDISC controlled terms)


```python
def consistency_checks(df):
    df = df.copy()
    df['SEX'] = df['SEX'].where(df['SEX'].isin(['M', 'F', 'U']), 'U')
    df['RACE'] = df['RACE'].astype(str)
    race_map = {
        'AMERICAN INDIAN OR ALASKA NATIVE': 'AMERICAN INDIAN/ALASKA NATIVE',
        'ASIAN': 'ASIAN',
        'BLACK OR AFRICAN AMERICAN': 'BLACK/AFRICAN AMERICAN',
        'NATIVE HAWAIIAN OR OTHER PACIFIC ISLANDER': 'NATIVE HAWAIIAN/OTHER PACIFIC ISLANDER',
        'WHITE': 'WHITE'
    }
    df['RACE'] = df['RACE'].replace(race_map)
    df['RACE'] = df['RACE'].astype('category')
    
    df['HIGH_RISK'] = ((df['AGE'] > 70) | 
                       (df['RACE'].str.contains('ASIAN|BLACK', na=False)) | 
                       (df['ARM_TYPE'] == 'Treatment')).astype(int)
    
    drop_cols = ['DOMAIN', 'SUBJID']
    df = df.drop(columns=[col for col in drop_cols if col in df.columns])
    return df
    
consistency_df = consistency_checks(clean_outliers_df)
consistency_df.head(5)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>STUDYID</th>
      <th>USUBJID</th>
      <th>RFSTDTC</th>
      <th>RFENDTC</th>
      <th>RFXSTDTC</th>
      <th>RFXENDTC</th>
      <th>RFICDTC</th>
      <th>RFPENDTC</th>
      <th>DTHDTC</th>
      <th>DTHFL</th>
      <th>...</th>
      <th>ARM</th>
      <th>ACTARMCD</th>
      <th>ACTARM</th>
      <th>COUNTRY</th>
      <th>DMDTC</th>
      <th>DMDY</th>
      <th>AGE_GROUP</th>
      <th>STUDY_DURATION</th>
      <th>ARM_TYPE</th>
      <th>HIGH_RISK</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>CDISCPILOT01</td>
      <td>01-701-1015</td>
      <td>2014-01-02</td>
      <td>2014-07-02</td>
      <td>2014-01-02</td>
      <td>2014-07-02</td>
      <td>NaT</td>
      <td>2014-07-02 11:45:00</td>
      <td>NaT</td>
      <td></td>
      <td>...</td>
      <td>Placebo</td>
      <td>Pbo</td>
      <td>Placebo</td>
      <td>USA</td>
      <td>2013-12-26</td>
      <td>-7</td>
      <td>Adult</td>
      <td>-7</td>
      <td>Control</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>CDISCPILOT01</td>
      <td>01-701-1023</td>
      <td>2012-08-05</td>
      <td>2012-09-02</td>
      <td>2012-08-05</td>
      <td>2012-09-01</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaT</td>
      <td></td>
      <td>...</td>
      <td>Placebo</td>
      <td>Pbo</td>
      <td>Placebo</td>
      <td>USA</td>
      <td>2012-07-22</td>
      <td>-14</td>
      <td>Adult</td>
      <td>-14</td>
      <td>Control</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>CDISCPILOT01</td>
      <td>01-701-1028</td>
      <td>2013-07-19</td>
      <td>2014-01-14</td>
      <td>2013-07-19</td>
      <td>2014-01-14</td>
      <td>NaT</td>
      <td>2014-01-14 11:10:00</td>
      <td>NaT</td>
      <td></td>
      <td>...</td>
      <td>Xanomeline High Dose</td>
      <td>Xan_Hi</td>
      <td>Xanomeline High Dose</td>
      <td>USA</td>
      <td>2013-07-11</td>
      <td>-8</td>
      <td>Senior</td>
      <td>-8</td>
      <td>Treatment</td>
      <td>1</td>
    </tr>
    <tr>
      <th>3</th>
      <td>CDISCPILOT01</td>
      <td>01-701-1033</td>
      <td>2014-03-18</td>
      <td>2014-04-14</td>
      <td>2014-03-18</td>
      <td>2014-03-31</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaT</td>
      <td></td>
      <td>...</td>
      <td>Xanomeline Low Dose</td>
      <td>Xan_Lo</td>
      <td>Xanomeline Low Dose</td>
      <td>USA</td>
      <td>2014-03-10</td>
      <td>-8</td>
      <td>Senior</td>
      <td>-8</td>
      <td>Treatment</td>
      <td>1</td>
    </tr>
    <tr>
      <th>4</th>
      <td>CDISCPILOT01</td>
      <td>01-701-1034</td>
      <td>2014-07-01</td>
      <td>2014-12-30</td>
      <td>2014-07-01</td>
      <td>2014-12-30</td>
      <td>NaT</td>
      <td>2014-12-30 09:50:00</td>
      <td>NaT</td>
      <td></td>
      <td>...</td>
      <td>Xanomeline High Dose</td>
      <td>Xan_Hi</td>
      <td>Xanomeline High Dose</td>
      <td>USA</td>
      <td>2014-06-24</td>
      <td>-7</td>
      <td>Senior</td>
      <td>-7</td>
      <td>Treatment</td>
      <td>1</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 27 columns</p>
</div>



### 7. Export Prep (reorder for DB, add audit)


```python
def prepare_export(df):
    """Step 7: Export Prep (reindex and timestamp)"""
    df = df.copy()
    col_order = [
        'USUBJID', 'STUDYID', 'SITEID', 'AGE', 'AGEU', 'SEX', 'RACE', 'ETHNIC', 'COUNTRY',
        'ARMCD', 'ARM', 'ACTARMCD', 'ACTARM', 'RFXSTDTC', 'RFXENDTC', 'DMDY', 'DMDTC',
        'AGE_GROUP', 'ARM_TYPE', 'STUDY_DURATION', 'HIGH_RISK'
    ]
    existing_cols = [col for col in col_order if col in df.columns]
    remaining_cols = [col for col in df.columns if col not in col_order]
    df = df.reindex(columns=existing_cols + remaining_cols)
    df['ETL_TIMESTAMP'] = pd.Timestamp.now()
    return df
    
prepare_export_df = prepare_export(consistency_df)
final_df = prepare_export_df
final_df.head(5)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>USUBJID</th>
      <th>STUDYID</th>
      <th>SITEID</th>
      <th>AGE</th>
      <th>AGEU</th>
      <th>SEX</th>
      <th>RACE</th>
      <th>ETHNIC</th>
      <th>COUNTRY</th>
      <th>ARMCD</th>
      <th>...</th>
      <th>ARM_TYPE</th>
      <th>STUDY_DURATION</th>
      <th>HIGH_RISK</th>
      <th>RFSTDTC</th>
      <th>RFENDTC</th>
      <th>RFICDTC</th>
      <th>RFPENDTC</th>
      <th>DTHDTC</th>
      <th>DTHFL</th>
      <th>ETL_TIMESTAMP</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>01-701-1015</td>
      <td>CDISCPILOT01</td>
      <td>701</td>
      <td>63</td>
      <td>YEARS</td>
      <td>F</td>
      <td>WHITE</td>
      <td>HISPANIC OR LATINO</td>
      <td>USA</td>
      <td>Pbo</td>
      <td>...</td>
      <td>Control</td>
      <td>-7</td>
      <td>0</td>
      <td>2014-01-02</td>
      <td>2014-07-02</td>
      <td>NaT</td>
      <td>2014-07-02 11:45:00</td>
      <td>NaT</td>
      <td></td>
      <td>2025-09-24 19:51:49.658021</td>
    </tr>
    <tr>
      <th>1</th>
      <td>01-701-1023</td>
      <td>CDISCPILOT01</td>
      <td>701</td>
      <td>64</td>
      <td>YEARS</td>
      <td>M</td>
      <td>WHITE</td>
      <td>HISPANIC OR LATINO</td>
      <td>USA</td>
      <td>Pbo</td>
      <td>...</td>
      <td>Control</td>
      <td>-14</td>
      <td>0</td>
      <td>2012-08-05</td>
      <td>2012-09-02</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaT</td>
      <td></td>
      <td>2025-09-24 19:51:49.658021</td>
    </tr>
    <tr>
      <th>2</th>
      <td>01-701-1028</td>
      <td>CDISCPILOT01</td>
      <td>701</td>
      <td>71</td>
      <td>YEARS</td>
      <td>M</td>
      <td>WHITE</td>
      <td>NOT HISPANIC OR LATINO</td>
      <td>USA</td>
      <td>Xan_Hi</td>
      <td>...</td>
      <td>Treatment</td>
      <td>-8</td>
      <td>1</td>
      <td>2013-07-19</td>
      <td>2014-01-14</td>
      <td>NaT</td>
      <td>2014-01-14 11:10:00</td>
      <td>NaT</td>
      <td></td>
      <td>2025-09-24 19:51:49.658021</td>
    </tr>
    <tr>
      <th>3</th>
      <td>01-701-1033</td>
      <td>CDISCPILOT01</td>
      <td>701</td>
      <td>74</td>
      <td>YEARS</td>
      <td>M</td>
      <td>WHITE</td>
      <td>NOT HISPANIC OR LATINO</td>
      <td>USA</td>
      <td>Xan_Lo</td>
      <td>...</td>
      <td>Treatment</td>
      <td>-8</td>
      <td>1</td>
      <td>2014-03-18</td>
      <td>2014-04-14</td>
      <td>NaT</td>
      <td>NaT</td>
      <td>NaT</td>
      <td></td>
      <td>2025-09-24 19:51:49.658021</td>
    </tr>
    <tr>
      <th>4</th>
      <td>01-701-1034</td>
      <td>CDISCPILOT01</td>
      <td>701</td>
      <td>77</td>
      <td>YEARS</td>
      <td>F</td>
      <td>WHITE</td>
      <td>NOT HISPANIC OR LATINO</td>
      <td>USA</td>
      <td>Xan_Hi</td>
      <td>...</td>
      <td>Treatment</td>
      <td>-7</td>
      <td>1</td>
      <td>2014-07-01</td>
      <td>2014-12-30</td>
      <td>NaT</td>
      <td>2014-12-30 09:50:00</td>
      <td>NaT</td>
      <td></td>
      <td>2025-09-24 19:51:49.658021</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 28 columns</p>
</div>



---

# AE.xpt Data

### Load raw AE.xpt


```python
def load_ae_data():
    data_path = r'updated-pilot-submission-package\900172\m5\datasets\cdiscpilot01\tabulations\sdtm'
    ae_df, ae_meta = pyreadstat.read_xport(os.path.join(data_path, 'ae.xpt'))
    return ae_df

ae_df = load_ae_data()
ae_df.head(5)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>STUDYID</th>
      <th>DOMAIN</th>
      <th>USUBJID</th>
      <th>AESEQ</th>
      <th>AESPID</th>
      <th>AETERM</th>
      <th>AELLT</th>
      <th>AELLTCD</th>
      <th>AEDECOD</th>
      <th>AEPTCD</th>
      <th>...</th>
      <th>AESDISAB</th>
      <th>AESDTH</th>
      <th>AESHOSP</th>
      <th>AESLIFE</th>
      <th>AESOD</th>
      <th>AEDTC</th>
      <th>AESTDTC</th>
      <th>AEENDTC</th>
      <th>AESTDY</th>
      <th>AEENDY</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>CDISCPILOT01</td>
      <td>AE</td>
      <td>01-701-1015</td>
      <td>1.0</td>
      <td>E07</td>
      <td>APPLICATION SITE ERYTHEMA</td>
      <td>APPLICATION SITE REDNESS</td>
      <td>NaN</td>
      <td>APPLICATION SITE ERYTHEMA</td>
      <td>NaN</td>
      <td>...</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>2014-01-16</td>
      <td>2014-01-03</td>
      <td></td>
      <td>2.0</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>CDISCPILOT01</td>
      <td>AE</td>
      <td>01-701-1015</td>
      <td>2.0</td>
      <td>E08</td>
      <td>APPLICATION SITE PRURITUS</td>
      <td>APPLICATION SITE ITCHING</td>
      <td>NaN</td>
      <td>APPLICATION SITE PRURITUS</td>
      <td>NaN</td>
      <td>...</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>2014-01-16</td>
      <td>2014-01-03</td>
      <td></td>
      <td>2.0</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>CDISCPILOT01</td>
      <td>AE</td>
      <td>01-701-1015</td>
      <td>3.0</td>
      <td>E06</td>
      <td>DIARRHOEA</td>
      <td>DIARRHEA</td>
      <td>NaN</td>
      <td>DIARRHOEA</td>
      <td>NaN</td>
      <td>...</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>2014-01-16</td>
      <td>2014-01-09</td>
      <td>2014-01-11</td>
      <td>8.0</td>
      <td>10.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>CDISCPILOT01</td>
      <td>AE</td>
      <td>01-701-1023</td>
      <td>3.0</td>
      <td>E10</td>
      <td>ATRIOVENTRICULAR BLOCK SECOND DEGREE</td>
      <td>AV BLOCK SECOND DEGREE</td>
      <td>NaN</td>
      <td>ATRIOVENTRICULAR BLOCK SECOND DEGREE</td>
      <td>NaN</td>
      <td>...</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>2012-08-27</td>
      <td>2012-08-26</td>
      <td></td>
      <td>22.0</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4</th>
      <td>CDISCPILOT01</td>
      <td>AE</td>
      <td>01-701-1023</td>
      <td>1.0</td>
      <td>E08</td>
      <td>ERYTHEMA</td>
      <td>ERYTHEMA</td>
      <td>NaN</td>
      <td>ERYTHEMA</td>
      <td>NaN</td>
      <td>...</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>2012-08-27</td>
      <td>2012-08-07</td>
      <td>2012-08-30</td>
      <td>3.0</td>
      <td>26.0</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 35 columns</p>
</div>



### 1. Missing Value Imputation


```python
def impute_missing_ae(df):
    df = df.copy()
    df['AETERM'] = df['AETERM'].fillna('Not Specified')
    df['AEDECOD'] = df['AEDECOD'].fillna('Not Specified')
    # Forward-fill dates by USUBJID (helps sequential events)
    df['AESTDTC'] = df.groupby('USUBJID')['AESTDTC'].ffill()
    df['AEENDTC'] = df.groupby('USUBJID')['AEENDTC'].ffill()
    # Impute day numbers (AESTDY/AEENDY) with per-group median if possible, else overall median to avoid empty slice warning
    for day_col in ['AESTDY', 'AEENDY']:
        if day_col in df.columns:
            overall_median = df[day_col].median()
            # Group median with fallback
            group_medians = df.groupby('USUBJID')[day_col].median()
            group_medians = group_medians.fillna(overall_median)
            df[day_col] = df[day_col].fillna(df['USUBJID'].map(group_medians))
            # Final fill if still NaN (rare)
            df[day_col] = df[day_col].fillna(overall_median)
    df = df.dropna(subset=['USUBJID'])
    print(f"Imputed days—missings left: AESTDY={df['AESTDY'].isna().sum() if 'AESTDY' in df else 'N/A'}, AEENDY={df['AEENDY'].isna().sum() if 'AEENDY' in df else 'N/A'}")
    return df

impute_missing_ae_df = impute_missing_ae(ae_df)
impute_missing_ae_df.head(5)
```

    Imputed days—missings left: AESTDY=0, AEENDY=0
    




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>STUDYID</th>
      <th>DOMAIN</th>
      <th>USUBJID</th>
      <th>AESEQ</th>
      <th>AESPID</th>
      <th>AETERM</th>
      <th>AELLT</th>
      <th>AELLTCD</th>
      <th>AEDECOD</th>
      <th>AEPTCD</th>
      <th>...</th>
      <th>AESDISAB</th>
      <th>AESDTH</th>
      <th>AESHOSP</th>
      <th>AESLIFE</th>
      <th>AESOD</th>
      <th>AEDTC</th>
      <th>AESTDTC</th>
      <th>AEENDTC</th>
      <th>AESTDY</th>
      <th>AEENDY</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>CDISCPILOT01</td>
      <td>AE</td>
      <td>01-701-1015</td>
      <td>1.0</td>
      <td>E07</td>
      <td>APPLICATION SITE ERYTHEMA</td>
      <td>APPLICATION SITE REDNESS</td>
      <td>NaN</td>
      <td>APPLICATION SITE ERYTHEMA</td>
      <td>NaN</td>
      <td>...</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>2014-01-16</td>
      <td>2014-01-03</td>
      <td></td>
      <td>2.0</td>
      <td>10.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>CDISCPILOT01</td>
      <td>AE</td>
      <td>01-701-1015</td>
      <td>2.0</td>
      <td>E08</td>
      <td>APPLICATION SITE PRURITUS</td>
      <td>APPLICATION SITE ITCHING</td>
      <td>NaN</td>
      <td>APPLICATION SITE PRURITUS</td>
      <td>NaN</td>
      <td>...</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>2014-01-16</td>
      <td>2014-01-03</td>
      <td></td>
      <td>2.0</td>
      <td>10.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>CDISCPILOT01</td>
      <td>AE</td>
      <td>01-701-1015</td>
      <td>3.0</td>
      <td>E06</td>
      <td>DIARRHOEA</td>
      <td>DIARRHEA</td>
      <td>NaN</td>
      <td>DIARRHOEA</td>
      <td>NaN</td>
      <td>...</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>2014-01-16</td>
      <td>2014-01-09</td>
      <td>2014-01-11</td>
      <td>8.0</td>
      <td>10.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>CDISCPILOT01</td>
      <td>AE</td>
      <td>01-701-1023</td>
      <td>3.0</td>
      <td>E10</td>
      <td>ATRIOVENTRICULAR BLOCK SECOND DEGREE</td>
      <td>AV BLOCK SECOND DEGREE</td>
      <td>NaN</td>
      <td>ATRIOVENTRICULAR BLOCK SECOND DEGREE</td>
      <td>NaN</td>
      <td>...</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>2012-08-27</td>
      <td>2012-08-26</td>
      <td></td>
      <td>22.0</td>
      <td>26.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>CDISCPILOT01</td>
      <td>AE</td>
      <td>01-701-1023</td>
      <td>1.0</td>
      <td>E08</td>
      <td>ERYTHEMA</td>
      <td>ERYTHEMA</td>
      <td>NaN</td>
      <td>ERYTHEMA</td>
      <td>NaN</td>
      <td>...</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>2012-08-27</td>
      <td>2012-08-07</td>
      <td>2012-08-30</td>
      <td>3.0</td>
      <td>26.0</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 35 columns</p>
</div>



### 2. Date Standardization


```python
def standardize_dates_ae(df):
    df = df.copy()
    # Parse all date cols (infer format for flexibility)
    date_cols = ['AEDTC', 'AESTDTC', 'AEENDTC']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    # Derived duration: Prefer date diff, fallback to rounded day diff
    date_diff = (df['AEENDTC'] - df['AESTDTC']).dt.days
    day_diff = (pd.to_numeric(df['AEENDY'], errors='coerce') - pd.to_numeric(df['AESTDY'], errors='coerce')).round()
    df['EVENT_DURATION'] = date_diff.fillna(day_diff)
    df['EVENT_DURATION'] = df['EVENT_DURATION'].fillna(0).astype('Int64')  # Safe cast after round
    print(f"Parsed dates—NaT counts: AEDTC={df['AEDTC'].isna().sum() if 'AEDTC' in df else 'N/A'}, AESTDTC={df['AESTDTC'].isna().sum()}, AEENDTC={df['AEENDTC'].isna().sum()}")
    print(f"Duration stats: mean={df['EVENT_DURATION'].mean():.1f}, min={df['EVENT_DURATION'].min()}")
    return df

standardize_dates_ae_df = standardize_dates_ae(impute_missing_ae_df)
standardize_dates_ae_df.head(5)
# standardize_dates_ae_df.columns
```

    Parsed dates—NaT counts: AEDTC=0, AESTDTC=26, AEENDTC=473
    Duration stats: mean=17.8, min=-284
    




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>STUDYID</th>
      <th>DOMAIN</th>
      <th>USUBJID</th>
      <th>AESEQ</th>
      <th>AESPID</th>
      <th>AETERM</th>
      <th>AELLT</th>
      <th>AELLTCD</th>
      <th>AEDECOD</th>
      <th>AEPTCD</th>
      <th>...</th>
      <th>AESDTH</th>
      <th>AESHOSP</th>
      <th>AESLIFE</th>
      <th>AESOD</th>
      <th>AEDTC</th>
      <th>AESTDTC</th>
      <th>AEENDTC</th>
      <th>AESTDY</th>
      <th>AEENDY</th>
      <th>EVENT_DURATION</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>CDISCPILOT01</td>
      <td>AE</td>
      <td>01-701-1015</td>
      <td>1.0</td>
      <td>E07</td>
      <td>APPLICATION SITE ERYTHEMA</td>
      <td>APPLICATION SITE REDNESS</td>
      <td>NaN</td>
      <td>APPLICATION SITE ERYTHEMA</td>
      <td>NaN</td>
      <td>...</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>2014-01-16</td>
      <td>2014-01-03</td>
      <td>NaT</td>
      <td>2.0</td>
      <td>10.0</td>
      <td>8</td>
    </tr>
    <tr>
      <th>1</th>
      <td>CDISCPILOT01</td>
      <td>AE</td>
      <td>01-701-1015</td>
      <td>2.0</td>
      <td>E08</td>
      <td>APPLICATION SITE PRURITUS</td>
      <td>APPLICATION SITE ITCHING</td>
      <td>NaN</td>
      <td>APPLICATION SITE PRURITUS</td>
      <td>NaN</td>
      <td>...</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>2014-01-16</td>
      <td>2014-01-03</td>
      <td>NaT</td>
      <td>2.0</td>
      <td>10.0</td>
      <td>8</td>
    </tr>
    <tr>
      <th>2</th>
      <td>CDISCPILOT01</td>
      <td>AE</td>
      <td>01-701-1015</td>
      <td>3.0</td>
      <td>E06</td>
      <td>DIARRHOEA</td>
      <td>DIARRHEA</td>
      <td>NaN</td>
      <td>DIARRHOEA</td>
      <td>NaN</td>
      <td>...</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>2014-01-16</td>
      <td>2014-01-09</td>
      <td>2014-01-11</td>
      <td>8.0</td>
      <td>10.0</td>
      <td>2</td>
    </tr>
    <tr>
      <th>3</th>
      <td>CDISCPILOT01</td>
      <td>AE</td>
      <td>01-701-1023</td>
      <td>3.0</td>
      <td>E10</td>
      <td>ATRIOVENTRICULAR BLOCK SECOND DEGREE</td>
      <td>AV BLOCK SECOND DEGREE</td>
      <td>NaN</td>
      <td>ATRIOVENTRICULAR BLOCK SECOND DEGREE</td>
      <td>NaN</td>
      <td>...</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>2012-08-27</td>
      <td>2012-08-26</td>
      <td>NaT</td>
      <td>22.0</td>
      <td>26.0</td>
      <td>4</td>
    </tr>
    <tr>
      <th>4</th>
      <td>CDISCPILOT01</td>
      <td>AE</td>
      <td>01-701-1023</td>
      <td>1.0</td>
      <td>E08</td>
      <td>ERYTHEMA</td>
      <td>ERYTHEMA</td>
      <td>NaN</td>
      <td>ERYTHEMA</td>
      <td>NaN</td>
      <td>...</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>2012-08-27</td>
      <td>2012-08-07</td>
      <td>2012-08-30</td>
      <td>3.0</td>
      <td>26.0</td>
      <td>23</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 36 columns</p>
</div>



### 3. Data Type Enforcement


```python
def enforce_data_types_ae(df):
    df = df.copy()
    # Day cols: Numeric -> round to int -> Int64 (handles floats/NaNs)
    for day_col in ['AESTDY', 'AEENDY']:
        if day_col in df.columns:
            df[day_col] = pd.to_numeric(df[day_col], errors='coerce').round(0).astype('Int64')
    # AESEV mapping to category (title case)
    if 'AESEV' in df.columns:
        severity_map = {'MILD': 'Mild', 'MODERATE': 'Moderate', 'SEVERE': 'Severe'}
        df['AESEV'] = df['AESEV'].str.upper().map(severity_map).fillna('Unknown')
        df['AESEV'] = df['AESEV'].astype('category')
    df['USUBJID'] = df['USUBJID'].astype(str)
    if 'AESER' in df.columns:
        df['AESER'] = df['AESER'].astype('category')  # Y/N
    print("Data types enforced—no errors expected.")
    print(f"Day cols post-fix: AESTDY dtype={df['AESTDY'].dtype if 'AESTDY' in df else 'N/A'}, sample={df['AESTDY'].head() if 'AESTDY' in df else 'N/A'}")
    return df

enforce_data_types_ae_df = enforce_data_types_ae(standardize_dates_ae_df)
enforce_data_types_ae_df.head(5)
```

    Data types enforced—no errors expected.
    Day cols post-fix: AESTDY dtype=Int64, sample=0     2
    1     2
    2     8
    3    22
    4     3
    Name: AESTDY, dtype: Int64
    




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>STUDYID</th>
      <th>DOMAIN</th>
      <th>USUBJID</th>
      <th>AESEQ</th>
      <th>AESPID</th>
      <th>AETERM</th>
      <th>AELLT</th>
      <th>AELLTCD</th>
      <th>AEDECOD</th>
      <th>AEPTCD</th>
      <th>...</th>
      <th>AESDTH</th>
      <th>AESHOSP</th>
      <th>AESLIFE</th>
      <th>AESOD</th>
      <th>AEDTC</th>
      <th>AESTDTC</th>
      <th>AEENDTC</th>
      <th>AESTDY</th>
      <th>AEENDY</th>
      <th>EVENT_DURATION</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>CDISCPILOT01</td>
      <td>AE</td>
      <td>01-701-1015</td>
      <td>1.0</td>
      <td>E07</td>
      <td>APPLICATION SITE ERYTHEMA</td>
      <td>APPLICATION SITE REDNESS</td>
      <td>NaN</td>
      <td>APPLICATION SITE ERYTHEMA</td>
      <td>NaN</td>
      <td>...</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>2014-01-16</td>
      <td>2014-01-03</td>
      <td>NaT</td>
      <td>2</td>
      <td>10</td>
      <td>8</td>
    </tr>
    <tr>
      <th>1</th>
      <td>CDISCPILOT01</td>
      <td>AE</td>
      <td>01-701-1015</td>
      <td>2.0</td>
      <td>E08</td>
      <td>APPLICATION SITE PRURITUS</td>
      <td>APPLICATION SITE ITCHING</td>
      <td>NaN</td>
      <td>APPLICATION SITE PRURITUS</td>
      <td>NaN</td>
      <td>...</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>2014-01-16</td>
      <td>2014-01-03</td>
      <td>NaT</td>
      <td>2</td>
      <td>10</td>
      <td>8</td>
    </tr>
    <tr>
      <th>2</th>
      <td>CDISCPILOT01</td>
      <td>AE</td>
      <td>01-701-1015</td>
      <td>3.0</td>
      <td>E06</td>
      <td>DIARRHOEA</td>
      <td>DIARRHEA</td>
      <td>NaN</td>
      <td>DIARRHOEA</td>
      <td>NaN</td>
      <td>...</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>2014-01-16</td>
      <td>2014-01-09</td>
      <td>2014-01-11</td>
      <td>8</td>
      <td>10</td>
      <td>2</td>
    </tr>
    <tr>
      <th>3</th>
      <td>CDISCPILOT01</td>
      <td>AE</td>
      <td>01-701-1023</td>
      <td>3.0</td>
      <td>E10</td>
      <td>ATRIOVENTRICULAR BLOCK SECOND DEGREE</td>
      <td>AV BLOCK SECOND DEGREE</td>
      <td>NaN</td>
      <td>ATRIOVENTRICULAR BLOCK SECOND DEGREE</td>
      <td>NaN</td>
      <td>...</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>2012-08-27</td>
      <td>2012-08-26</td>
      <td>NaT</td>
      <td>22</td>
      <td>26</td>
      <td>4</td>
    </tr>
    <tr>
      <th>4</th>
      <td>CDISCPILOT01</td>
      <td>AE</td>
      <td>01-701-1023</td>
      <td>1.0</td>
      <td>E08</td>
      <td>ERYTHEMA</td>
      <td>ERYTHEMA</td>
      <td>NaN</td>
      <td>ERYTHEMA</td>
      <td>NaN</td>
      <td>...</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>2012-08-27</td>
      <td>2012-08-07</td>
      <td>2012-08-30</td>
      <td>3</td>
      <td>26</td>
      <td>23</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 36 columns</p>
</div>



### 4. Derived Variables


```python
def derive_variables_ae(df):
    df = df.copy()
    # High-Grade proxy: 1 if SEVERE (since no AETOXGR)
    if 'AESEV' in df.columns:
        df['HIGH_GRADE'] = (df['AESEV'] == 'Severe').astype(int)
    else:
        df['HIGH_GRADE'] = 0
    # Events per subject
    df['EVENT_COUNT_PER_SUBJ'] = df.groupby('USUBJID')['USUBJID'].transform('size')
    # Serious flag from AESER
    if 'AESER' in df.columns:
        df['SERIOUS_EVENT'] = (df['AESER'] == 'Y').astype(int)
    else:
        df['SERIOUS_EVENT'] = 0
    print(f"Derived stats: HIGH_GRADE=1 count={df['HIGH_GRADE'].sum()}, SERIOUS_EVENT=1 count={df['SERIOUS_EVENT'].sum()}")
    return df

derive_variables_ae_df = derive_variables_ae(enforce_data_types_ae_df)
derive_variables_ae_df.head(5)
```

    Derived stats: HIGH_GRADE=1 count=43, SERIOUS_EVENT=1 count=3
    




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>STUDYID</th>
      <th>DOMAIN</th>
      <th>USUBJID</th>
      <th>AESEQ</th>
      <th>AESPID</th>
      <th>AETERM</th>
      <th>AELLT</th>
      <th>AELLTCD</th>
      <th>AEDECOD</th>
      <th>AEPTCD</th>
      <th>...</th>
      <th>AESOD</th>
      <th>AEDTC</th>
      <th>AESTDTC</th>
      <th>AEENDTC</th>
      <th>AESTDY</th>
      <th>AEENDY</th>
      <th>EVENT_DURATION</th>
      <th>HIGH_GRADE</th>
      <th>EVENT_COUNT_PER_SUBJ</th>
      <th>SERIOUS_EVENT</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>CDISCPILOT01</td>
      <td>AE</td>
      <td>01-701-1015</td>
      <td>1.0</td>
      <td>E07</td>
      <td>APPLICATION SITE ERYTHEMA</td>
      <td>APPLICATION SITE REDNESS</td>
      <td>NaN</td>
      <td>APPLICATION SITE ERYTHEMA</td>
      <td>NaN</td>
      <td>...</td>
      <td>N</td>
      <td>2014-01-16</td>
      <td>2014-01-03</td>
      <td>NaT</td>
      <td>2</td>
      <td>10</td>
      <td>8</td>
      <td>0</td>
      <td>3</td>
      <td>0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>CDISCPILOT01</td>
      <td>AE</td>
      <td>01-701-1015</td>
      <td>2.0</td>
      <td>E08</td>
      <td>APPLICATION SITE PRURITUS</td>
      <td>APPLICATION SITE ITCHING</td>
      <td>NaN</td>
      <td>APPLICATION SITE PRURITUS</td>
      <td>NaN</td>
      <td>...</td>
      <td>N</td>
      <td>2014-01-16</td>
      <td>2014-01-03</td>
      <td>NaT</td>
      <td>2</td>
      <td>10</td>
      <td>8</td>
      <td>0</td>
      <td>3</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>CDISCPILOT01</td>
      <td>AE</td>
      <td>01-701-1015</td>
      <td>3.0</td>
      <td>E06</td>
      <td>DIARRHOEA</td>
      <td>DIARRHEA</td>
      <td>NaN</td>
      <td>DIARRHOEA</td>
      <td>NaN</td>
      <td>...</td>
      <td>N</td>
      <td>2014-01-16</td>
      <td>2014-01-09</td>
      <td>2014-01-11</td>
      <td>8</td>
      <td>10</td>
      <td>2</td>
      <td>0</td>
      <td>3</td>
      <td>0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>CDISCPILOT01</td>
      <td>AE</td>
      <td>01-701-1023</td>
      <td>3.0</td>
      <td>E10</td>
      <td>ATRIOVENTRICULAR BLOCK SECOND DEGREE</td>
      <td>AV BLOCK SECOND DEGREE</td>
      <td>NaN</td>
      <td>ATRIOVENTRICULAR BLOCK SECOND DEGREE</td>
      <td>NaN</td>
      <td>...</td>
      <td>N</td>
      <td>2012-08-27</td>
      <td>2012-08-26</td>
      <td>NaT</td>
      <td>22</td>
      <td>26</td>
      <td>4</td>
      <td>0</td>
      <td>4</td>
      <td>0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>CDISCPILOT01</td>
      <td>AE</td>
      <td>01-701-1023</td>
      <td>1.0</td>
      <td>E08</td>
      <td>ERYTHEMA</td>
      <td>ERYTHEMA</td>
      <td>NaN</td>
      <td>ERYTHEMA</td>
      <td>NaN</td>
      <td>...</td>
      <td>N</td>
      <td>2012-08-27</td>
      <td>2012-08-07</td>
      <td>2012-08-30</td>
      <td>3</td>
      <td>26</td>
      <td>23</td>
      <td>0</td>
      <td>4</td>
      <td>0</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 39 columns</p>
</div>



### 5. Outlier Detection/Cleaning


```python
def clean_outliers_ae(df):
    df = df.copy()
    print("Before filters:", df.shape)
    # Remove negative durations
    df = df[df['EVENT_DURATION'] >= 0]
    print("After duration filter:", df.shape)
    # Dedup by USUBJID + AESTDTC
    df = df.drop_duplicates(subset=['USUBJID', 'AESTDTC'])
    print("After duplicates:", df.shape)
    return df

clean_outliers_ae_df = clean_outliers_ae(derive_variables_ae_df)
clean_outliers_ae_df.head(5)
```

    Before filters: (1191, 39)
    After duration filter: (1012, 39)
    After duplicates: (553, 39)
    




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>STUDYID</th>
      <th>DOMAIN</th>
      <th>USUBJID</th>
      <th>AESEQ</th>
      <th>AESPID</th>
      <th>AETERM</th>
      <th>AELLT</th>
      <th>AELLTCD</th>
      <th>AEDECOD</th>
      <th>AEPTCD</th>
      <th>...</th>
      <th>AESOD</th>
      <th>AEDTC</th>
      <th>AESTDTC</th>
      <th>AEENDTC</th>
      <th>AESTDY</th>
      <th>AEENDY</th>
      <th>EVENT_DURATION</th>
      <th>HIGH_GRADE</th>
      <th>EVENT_COUNT_PER_SUBJ</th>
      <th>SERIOUS_EVENT</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>CDISCPILOT01</td>
      <td>AE</td>
      <td>01-701-1015</td>
      <td>1.0</td>
      <td>E07</td>
      <td>APPLICATION SITE ERYTHEMA</td>
      <td>APPLICATION SITE REDNESS</td>
      <td>NaN</td>
      <td>APPLICATION SITE ERYTHEMA</td>
      <td>NaN</td>
      <td>...</td>
      <td>N</td>
      <td>2014-01-16</td>
      <td>2014-01-03</td>
      <td>NaT</td>
      <td>2</td>
      <td>10</td>
      <td>8</td>
      <td>0</td>
      <td>3</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>CDISCPILOT01</td>
      <td>AE</td>
      <td>01-701-1015</td>
      <td>3.0</td>
      <td>E06</td>
      <td>DIARRHOEA</td>
      <td>DIARRHEA</td>
      <td>NaN</td>
      <td>DIARRHOEA</td>
      <td>NaN</td>
      <td>...</td>
      <td>N</td>
      <td>2014-01-16</td>
      <td>2014-01-09</td>
      <td>2014-01-11</td>
      <td>8</td>
      <td>10</td>
      <td>2</td>
      <td>0</td>
      <td>3</td>
      <td>0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>CDISCPILOT01</td>
      <td>AE</td>
      <td>01-701-1023</td>
      <td>3.0</td>
      <td>E10</td>
      <td>ATRIOVENTRICULAR BLOCK SECOND DEGREE</td>
      <td>AV BLOCK SECOND DEGREE</td>
      <td>NaN</td>
      <td>ATRIOVENTRICULAR BLOCK SECOND DEGREE</td>
      <td>NaN</td>
      <td>...</td>
      <td>N</td>
      <td>2012-08-27</td>
      <td>2012-08-26</td>
      <td>NaT</td>
      <td>22</td>
      <td>26</td>
      <td>4</td>
      <td>0</td>
      <td>4</td>
      <td>0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>CDISCPILOT01</td>
      <td>AE</td>
      <td>01-701-1023</td>
      <td>1.0</td>
      <td>E08</td>
      <td>ERYTHEMA</td>
      <td>ERYTHEMA</td>
      <td>NaN</td>
      <td>ERYTHEMA</td>
      <td>NaN</td>
      <td>...</td>
      <td>N</td>
      <td>2012-08-27</td>
      <td>2012-08-07</td>
      <td>2012-08-30</td>
      <td>3</td>
      <td>26</td>
      <td>23</td>
      <td>0</td>
      <td>4</td>
      <td>0</td>
    </tr>
    <tr>
      <th>7</th>
      <td>CDISCPILOT01</td>
      <td>AE</td>
      <td>01-701-1028</td>
      <td>1.0</td>
      <td>E04</td>
      <td>APPLICATION SITE ERYTHEMA</td>
      <td>APPLICATION SITE ERYTHEMA</td>
      <td>NaN</td>
      <td>APPLICATION SITE ERYTHEMA</td>
      <td>NaN</td>
      <td>...</td>
      <td>N</td>
      <td>2013-08-01</td>
      <td>2013-07-21</td>
      <td>NaT</td>
      <td>3</td>
      <td>53</td>
      <td>50</td>
      <td>0</td>
      <td>2</td>
      <td>0</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 39 columns</p>
</div>



### 6. Consistency Checks


```python
def consistency_checks_ae(df):
    df = df.copy()
    # Basic MedDRA validation (substring for demo; keep if contains 'MedDRA' or as-is)
    if 'AEDECOD' in df.columns:
        df['AEDECOD'] = df['AEDECOD'].where(df['AEDECOD'].str.contains('MedDRA', na=False, case=False), df['AEDECOD'])
    # Ensure AEBODSYS non-null
    if 'AEBODSYS' in df.columns:
        df = df.dropna(subset=['AEBODSYS'])
    return df

consistency_checks_ae_df = consistency_checks_ae(clean_outliers_ae_df)
consistency_checks_ae_df.head(5)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>STUDYID</th>
      <th>DOMAIN</th>
      <th>USUBJID</th>
      <th>AESEQ</th>
      <th>AESPID</th>
      <th>AETERM</th>
      <th>AELLT</th>
      <th>AELLTCD</th>
      <th>AEDECOD</th>
      <th>AEPTCD</th>
      <th>...</th>
      <th>AESOD</th>
      <th>AEDTC</th>
      <th>AESTDTC</th>
      <th>AEENDTC</th>
      <th>AESTDY</th>
      <th>AEENDY</th>
      <th>EVENT_DURATION</th>
      <th>HIGH_GRADE</th>
      <th>EVENT_COUNT_PER_SUBJ</th>
      <th>SERIOUS_EVENT</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>CDISCPILOT01</td>
      <td>AE</td>
      <td>01-701-1015</td>
      <td>1.0</td>
      <td>E07</td>
      <td>APPLICATION SITE ERYTHEMA</td>
      <td>APPLICATION SITE REDNESS</td>
      <td>NaN</td>
      <td>APPLICATION SITE ERYTHEMA</td>
      <td>NaN</td>
      <td>...</td>
      <td>N</td>
      <td>2014-01-16</td>
      <td>2014-01-03</td>
      <td>NaT</td>
      <td>2</td>
      <td>10</td>
      <td>8</td>
      <td>0</td>
      <td>3</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>CDISCPILOT01</td>
      <td>AE</td>
      <td>01-701-1015</td>
      <td>3.0</td>
      <td>E06</td>
      <td>DIARRHOEA</td>
      <td>DIARRHEA</td>
      <td>NaN</td>
      <td>DIARRHOEA</td>
      <td>NaN</td>
      <td>...</td>
      <td>N</td>
      <td>2014-01-16</td>
      <td>2014-01-09</td>
      <td>2014-01-11</td>
      <td>8</td>
      <td>10</td>
      <td>2</td>
      <td>0</td>
      <td>3</td>
      <td>0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>CDISCPILOT01</td>
      <td>AE</td>
      <td>01-701-1023</td>
      <td>3.0</td>
      <td>E10</td>
      <td>ATRIOVENTRICULAR BLOCK SECOND DEGREE</td>
      <td>AV BLOCK SECOND DEGREE</td>
      <td>NaN</td>
      <td>ATRIOVENTRICULAR BLOCK SECOND DEGREE</td>
      <td>NaN</td>
      <td>...</td>
      <td>N</td>
      <td>2012-08-27</td>
      <td>2012-08-26</td>
      <td>NaT</td>
      <td>22</td>
      <td>26</td>
      <td>4</td>
      <td>0</td>
      <td>4</td>
      <td>0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>CDISCPILOT01</td>
      <td>AE</td>
      <td>01-701-1023</td>
      <td>1.0</td>
      <td>E08</td>
      <td>ERYTHEMA</td>
      <td>ERYTHEMA</td>
      <td>NaN</td>
      <td>ERYTHEMA</td>
      <td>NaN</td>
      <td>...</td>
      <td>N</td>
      <td>2012-08-27</td>
      <td>2012-08-07</td>
      <td>2012-08-30</td>
      <td>3</td>
      <td>26</td>
      <td>23</td>
      <td>0</td>
      <td>4</td>
      <td>0</td>
    </tr>
    <tr>
      <th>7</th>
      <td>CDISCPILOT01</td>
      <td>AE</td>
      <td>01-701-1028</td>
      <td>1.0</td>
      <td>E04</td>
      <td>APPLICATION SITE ERYTHEMA</td>
      <td>APPLICATION SITE ERYTHEMA</td>
      <td>NaN</td>
      <td>APPLICATION SITE ERYTHEMA</td>
      <td>NaN</td>
      <td>...</td>
      <td>N</td>
      <td>2013-08-01</td>
      <td>2013-07-21</td>
      <td>NaT</td>
      <td>3</td>
      <td>53</td>
      <td>50</td>
      <td>0</td>
      <td>2</td>
      <td>0</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 39 columns</p>
</div>



### 7. Export Prep


```python
def prepare_export_ae(df):
    df = df.copy()
    col_order = ['USUBJID', 'AESTDTC', 'AEENDTC', 'AETERM', 'AEDECOD', 'AETOXGR', 'HIGH_GRADE', 'EVENT_DURATION', 'SERIOUS_EVENT', 'EVENT_COUNT_PER_SUBJ']
    existing_cols = [col for col in col_order if col in df.columns]
    remaining_cols = [col for col in df.columns if col not in col_order]
    df = df.reindex(columns=existing_cols + remaining_cols)
    df['ETL_TIMESTAMP'] = pd.Timestamp.now()
    # Oncology risk flag
    if 'AETERM' in df.columns:
        df['ONCOLOGY_RISK'] = df['AETERM'].str.contains('neoplasm|cancer|tumor', case=False, na=False).astype(int)
    return df

prepare_export_ae_df = prepare_export_ae(consistency_checks_ae_df)
prepare_export_ae_df.head(5)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>USUBJID</th>
      <th>AESTDTC</th>
      <th>AEENDTC</th>
      <th>AETERM</th>
      <th>AEDECOD</th>
      <th>HIGH_GRADE</th>
      <th>EVENT_DURATION</th>
      <th>SERIOUS_EVENT</th>
      <th>EVENT_COUNT_PER_SUBJ</th>
      <th>STUDYID</th>
      <th>...</th>
      <th>AESDISAB</th>
      <th>AESDTH</th>
      <th>AESHOSP</th>
      <th>AESLIFE</th>
      <th>AESOD</th>
      <th>AEDTC</th>
      <th>AESTDY</th>
      <th>AEENDY</th>
      <th>ETL_TIMESTAMP</th>
      <th>ONCOLOGY_RISK</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>01-701-1015</td>
      <td>2014-01-03</td>
      <td>NaT</td>
      <td>APPLICATION SITE ERYTHEMA</td>
      <td>APPLICATION SITE ERYTHEMA</td>
      <td>0</td>
      <td>8</td>
      <td>0</td>
      <td>3</td>
      <td>CDISCPILOT01</td>
      <td>...</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>2014-01-16</td>
      <td>2</td>
      <td>10</td>
      <td>2025-09-25 00:57:23.624132</td>
      <td>0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>01-701-1015</td>
      <td>2014-01-09</td>
      <td>2014-01-11</td>
      <td>DIARRHOEA</td>
      <td>DIARRHOEA</td>
      <td>0</td>
      <td>2</td>
      <td>0</td>
      <td>3</td>
      <td>CDISCPILOT01</td>
      <td>...</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>2014-01-16</td>
      <td>8</td>
      <td>10</td>
      <td>2025-09-25 00:57:23.624132</td>
      <td>0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>01-701-1023</td>
      <td>2012-08-26</td>
      <td>NaT</td>
      <td>ATRIOVENTRICULAR BLOCK SECOND DEGREE</td>
      <td>ATRIOVENTRICULAR BLOCK SECOND DEGREE</td>
      <td>0</td>
      <td>4</td>
      <td>0</td>
      <td>4</td>
      <td>CDISCPILOT01</td>
      <td>...</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>2012-08-27</td>
      <td>22</td>
      <td>26</td>
      <td>2025-09-25 00:57:23.624132</td>
      <td>0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>01-701-1023</td>
      <td>2012-08-07</td>
      <td>2012-08-30</td>
      <td>ERYTHEMA</td>
      <td>ERYTHEMA</td>
      <td>0</td>
      <td>23</td>
      <td>0</td>
      <td>4</td>
      <td>CDISCPILOT01</td>
      <td>...</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>2012-08-27</td>
      <td>3</td>
      <td>26</td>
      <td>2025-09-25 00:57:23.624132</td>
      <td>0</td>
    </tr>
    <tr>
      <th>7</th>
      <td>01-701-1028</td>
      <td>2013-07-21</td>
      <td>NaT</td>
      <td>APPLICATION SITE ERYTHEMA</td>
      <td>APPLICATION SITE ERYTHEMA</td>
      <td>0</td>
      <td>50</td>
      <td>0</td>
      <td>2</td>
      <td>CDISCPILOT01</td>
      <td>...</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>N</td>
      <td>2013-08-01</td>
      <td>3</td>
      <td>53</td>
      <td>2025-09-25 00:57:23.624132</td>
      <td>0</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 41 columns</p>
</div>


