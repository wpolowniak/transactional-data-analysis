# Analyze Purchase Records to Determine Sales-Tax Engine Solution
The purpose of this analysis is to analyze purchase records to locate transactions within states that collect sales tax.

I found a dataset called `iowa_liquor_sales` in Google's BigQuery Public Data Set which contains ~22.2 million records of transactional liqor sales data in the state of Iowa by county. I extracted 7 million rows of data for the purposes of this analysis.

This dataset will serve as a proxy, wherein I will analyze liquor sale transactions across counties in Iowa instead of technology sales transactions in states accross the United States. My goal with this analysis is to demonstrate analytics, SQL and data cleaning skills.

![Visualization](./images/visualization.png)

```python
# import of necessary libraries
import pandas as pd
from dask import dataframe as ddf
from pathlib import Path
from sqlalchemy import create_engine
from time import time
import altair as alt
```

# Get data in usable format

Due to restrictions in how much data can be downloaded into a `.csv` file from BigQuery, I extracted the 7 million rows of data into three separate files.

Each of these files was loaded into a `sqlite` database table called `transactions` after performing a bit of data cleanup.
* Set correct data types for each column
* Set datetime data type for date columns
* Normalize county data so that all appear in uppercase, to avoid duplicates

I used the `dask` library to speed up computation time since 7 million rows is enough to bog down pandas. Getting all the data into a sqlite database helps to free up memory on my computer and allows me to do aggregations on the data and get it into bite-sized chunks to run an analysis.


```python
# create the sql engine to interact with the database
db = 'sqlite:///database.db'
engine = create_engine(db, echo=False)
```


```python
# specify the datatypes of columns to be read from csv files
dtypes = {
    'invoice_and_item_number':str,
    'store_number':int,
    'store_name':str,
    'address':str,
    'city':str,
    'zip_code':str,
    'store_location':str,
    'county_number':str,
    'county':str,
    'category':str,
    'category_name':str,
    'vendor_number':str,
    'vendor_name':str,
    'item_number':str,
    'item_description':str,
    'pack':int,
    'bottle_volume_ml':int,
    'state_bottle_cost':float,
    'state_bottle_retail':float,
    'bottles_sold':int,
    'sale_dollars':float,
    'volume_sold_liters':float,
    'volume_sold_gallons':float
}

date_cols = ['date']
```


```python
# identify files to be processed
file_names = ['transactions.csv', 'transactions2.csv', 'transactions3.csv']

# load each file into transactions table in sqlite database
start_time = time()
for file in file_names:
    print(f'working on {file}...')
    
    print(f'\topening file')
    loc = Path.cwd().parent / 'data' / file
    df_dask = ddf.read_csv(loc, dtype=dtypes, parse_dates=date_cols)
    
    # clean up the 'county' data to standardize as all uppercase
    df_dask['county'] = df_dask['county'].str.upper()
    
    print(f'\twriting to sql database')
    df_dask.to_sql('transactions', db, if_exists='append', index=False)
end_time = time()
print(f'{round(end_time - start_time, 2)} seconds elapsed')
```

    working on transactions.csv...
    	opening file
    	writing to sql database
    working on transactions2.csv...
    	opening file
    	writing to sql database
    working on transactions3.csv...
    	opening file
    	writing to sql database
    264.56 seconds elapsed
    


```python
# test to see if the load was successful - expecting 7M records
pd.read_sql_query('SELECT COUNT(*) FROM transactions', engine)
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
      <th>COUNT(*)</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>7000000</td>
    </tr>
  </tbody>
</table>
</div>



Now I have 7 million rows of transactional data in a database called `database`, in a table called `transactions`. Below is a preview.


```python
pd.read_sql_query("SELECT * FROM transactions LIMIT 2", engine)
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
      <th>invoice_and_item_number</th>
      <th>date</th>
      <th>store_number</th>
      <th>store_name</th>
      <th>address</th>
      <th>city</th>
      <th>zip_code</th>
      <th>store_location</th>
      <th>county_number</th>
      <th>county</th>
      <th>...</th>
      <th>item_number</th>
      <th>item_description</th>
      <th>pack</th>
      <th>bottle_volume_ml</th>
      <th>state_bottle_cost</th>
      <th>state_bottle_retail</th>
      <th>bottles_sold</th>
      <th>sale_dollars</th>
      <th>volume_sold_liters</th>
      <th>volume_sold_gallons</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>INV-30243700036</td>
      <td>2020-09-15 00:00:00.000000</td>
      <td>4973</td>
      <td>Quillins Quality Foods West Union</td>
      <td>303, Hwy 150 N</td>
      <td>West Union</td>
      <td>52175</td>
      <td>POINT (-91.814402 42.965065)</td>
      <td>33</td>
      <td>FAYETTE</td>
      <td>...</td>
      <td>33717</td>
      <td>Paramount Sloe Gin</td>
      <td>12</td>
      <td>1000</td>
      <td>5.42</td>
      <td>8.13</td>
      <td>4</td>
      <td>32.52</td>
      <td>4.0</td>
      <td>1.05</td>
    </tr>
    <tr>
      <th>1</th>
      <td>INV-35778400094</td>
      <td>2021-04-13 00:00:00.000000</td>
      <td>3723</td>
      <td>J D Spirits Liquor</td>
      <td>1023  9th St</td>
      <td>Onawa</td>
      <td>51040.0</td>
      <td>POINT (-96.095845 42.025841)</td>
      <td>67</td>
      <td>MONONA</td>
      <td>...</td>
      <td>33697</td>
      <td>Mr Boston Sloe Gin</td>
      <td>12</td>
      <td>1000</td>
      <td>5.42</td>
      <td>8.13</td>
      <td>4</td>
      <td>32.52</td>
      <td>4.0</td>
      <td>1.05</td>
    </tr>
  </tbody>
</table>
<p>2 rows × 24 columns</p>
</div>



# Analysis - Analyze Purchase Records
Individual transaction records are too granular. Now that I have the data stored in a SQL database, I can reduce the complexity and size of the data by running aggregation functions via SQL queries.

I will create a summary table aggregating the data:
* By month, county, and item
* Aggregate total sales dollars, total cost, count of transactions


```python
# summarize the data
query = """
    SELECT
        strftime('%Y-%m', date) month,
        county,
        item_number,
        item_description,
        SUM(sale_dollars) sale_dollars,
        SUM(state_bottle_cost * bottles_sold) cost,
        COUNT(*) num_transactions
        
    FROM transactions
    
    WHERE county IS NOT NULL
    
    GROUP BY
        1, 2, 3, 4
"""
df = pd.read_sql_query(query, engine)
```


```python
# check the size of the table
df.shape
```




    (1948988, 7)




```python
# preview the first five rows of the table
df.head()
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
      <th>month</th>
      <th>county</th>
      <th>item_number</th>
      <th>item_description</th>
      <th>sale_dollars</th>
      <th>cost</th>
      <th>num_transactions</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2012-01</td>
      <td>ADAIR</td>
      <td>15644</td>
      <td>Jameson</td>
      <td>79.11</td>
      <td>52.74</td>
      <td>3</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2012-01</td>
      <td>ADAIR</td>
      <td>27102</td>
      <td>Templeton Rye</td>
      <td>813.90</td>
      <td>542.40</td>
      <td>5</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2012-01</td>
      <td>ADAIR</td>
      <td>33716</td>
      <td>Paramount Sloe Gin</td>
      <td>46.00</td>
      <td>30.72</td>
      <td>2</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2012-01</td>
      <td>ADAIR</td>
      <td>34001</td>
      <td>Absolut Swedish Vodka 80 Prf Mini</td>
      <td>11.88</td>
      <td>7.92</td>
      <td>1</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2012-01</td>
      <td>ADAIR</td>
      <td>34006</td>
      <td>Absolut Swedish Vodka 80 Prf</td>
      <td>261.28</td>
      <td>174.24</td>
      <td>2</td>
    </tr>
  </tbody>
</table>
</div>



While this is still a large dataset, it's much smaller and easier to work with aggregate data at ~2M rows compared to 7M rows.

I will add a new table called `summary` to the SQL database in case I'll want to work with it later, and clear out my memory so my computer doesn't slow down.


```python
# add to sql database
df.to_sql('summary', engine, index=False, if_exists='replace')
```

## What do sales look like for taxable states?
Let's say I worked with the internal accounting group to identify counties (or states, municipalities, etc...) that are charging a sales tax which we will need to collect. I'd like to get a quick view of what sales look like by taxable vs non-taxable state.

To do this I will only look at the last 12 months of transactions and create a visualization of sales by county, with an indication of whether the county is taxable or not.


```python
# filter down my dataset to only capture trailing 12 months of sales
county_sales_t12m = (
    df.loc[df['month'] >= '2020-10']
    .groupby(['county'], as_index=False)
    .agg({'sale_dollars':sum, 'num_transactions':sum})
)
```


```python
# divide the total sales by 1,000,000 so that labels are prettier
county_sales_t12m['sale_dollars_m'] = county_sales_t12m['sale_dollars'] / 1000000
```


```python
# open the file on which I worked with accounting to identify taxable counties
loc = Path.cwd().parent / 'data' / 'tax_counties.xlsx'
tax_counties_df = pd.read_excel(loc)
```


```python
# merge with sales dataset, so that I can visualize it all together
county_sales_t12m = county_sales_t12m.merge(tax_counties_df, how='left', on='county')
```


```python
# create a visualization
bars = alt.Chart(county_sales_t12m).mark_bar().encode(
    x=alt.X('sale_dollars_m:Q', title='Sales ($mm)', axis=alt.Axis(orient='top')),
    y=alt.Y('county', sort=alt.EncodingSortField(field='sale_dollars_m', op="sum",order='descending'), title='County'),
    color=alt.Color('tax_flag', legend=alt.Legend(title='Tax Flag', orient='left'))
)

text = bars.mark_text(
    align='left',
    baseline='middle',
    dx=3
).encode(
    text=alt.Text('sale_dollars_m', format='$.2f')
)

chart = (bars + text)

chart.properties(
    title={
      "text": ["Trailing 12 Month Sales by County"], 
      "subtitle": ["Taxable vs. Non-Taxable"],
      "subtitleColor": "gray",
    }
).configure_title(
    anchor='start'
)
```





<div id="altair-viz-23a92e3bb03b49d99a9367e12c4bbf85"></div>
<script type="text/javascript">
  (function(spec, embedOpt){
    let outputDiv = document.currentScript.previousElementSibling;
    if (outputDiv.id !== "altair-viz-23a92e3bb03b49d99a9367e12c4bbf85") {
      outputDiv = document.getElementById("altair-viz-23a92e3bb03b49d99a9367e12c4bbf85");
    }
    const paths = {
      "vega": "https://cdn.jsdelivr.net/npm//vega@5?noext",
      "vega-lib": "https://cdn.jsdelivr.net/npm//vega-lib?noext",
      "vega-lite": "https://cdn.jsdelivr.net/npm//vega-lite@4.8.1?noext",
      "vega-embed": "https://cdn.jsdelivr.net/npm//vega-embed@6?noext",
    };

    function loadScript(lib) {
      return new Promise(function(resolve, reject) {
        var s = document.createElement('script');
        s.src = paths[lib];
        s.async = true;
        s.onload = () => resolve(paths[lib]);
        s.onerror = () => reject(`Error loading script: ${paths[lib]}`);
        document.getElementsByTagName("head")[0].appendChild(s);
      });
    }

    function showError(err) {
      outputDiv.innerHTML = `<div class="error" style="color:red;">${err}</div>`;
      throw err;
    }

    function displayChart(vegaEmbed) {
      vegaEmbed(outputDiv, spec, embedOpt)
        .catch(err => showError(`Javascript Error: ${err.message}<br>This usually means there's a typo in your chart specification. See the javascript console for the full traceback.`));
    }

    if(typeof define === "function" && define.amd) {
      requirejs.config({paths});
      require(["vega-embed"], displayChart, err => showError(`Error loading script: ${err.message}`));
    } else if (typeof vegaEmbed === "function") {
      displayChart(vegaEmbed);
    } else {
      loadScript("vega")
        .then(() => loadScript("vega-lite"))
        .then(() => loadScript("vega-embed"))
        .catch(showError)
        .then(() => displayChart(vegaEmbed));
    }
  })({"config": {"view": {"continuousWidth": 400, "continuousHeight": 300}, "title": {"anchor": "start"}}, "layer": [{"mark": "bar", "encoding": {"color": {"type": "nominal", "field": "tax_flag", "legend": {"orient": "left", "title": "Tax Flag"}}, "x": {"type": "quantitative", "axis": {"orient": "top"}, "field": "sale_dollars_m", "title": "Sales ($mm)"}, "y": {"type": "nominal", "field": "county", "sort": {"field": "sale_dollars_m", "op": "sum", "order": "descending"}, "title": "County"}}}, {"mark": {"type": "text", "align": "left", "baseline": "middle", "dx": 3}, "encoding": {"color": {"type": "nominal", "field": "tax_flag", "legend": {"orient": "left", "title": "Tax Flag"}}, "text": {"type": "quantitative", "field": "sale_dollars_m", "format": "$.2f"}, "x": {"type": "quantitative", "axis": {"orient": "top"}, "field": "sale_dollars_m", "title": "Sales ($mm)"}, "y": {"type": "nominal", "field": "county", "sort": {"field": "sale_dollars_m", "op": "sum", "order": "descending"}, "title": "County"}}}], "data": {"name": "data-7bff0cdbacc8d71fa9f0effecb292013"}, "title": {"text": ["Trailing 12 Month Sales by County"], "subtitle": ["Taxable vs. Non-Taxable"], "subtitleColor": "gray"}, "$schema": "https://vega.github.io/schema/vega-lite/v4.8.1.json", "datasets": {"data-7bff0cdbacc8d71fa9f0effecb292013": [{"county": "ADAIR", "sale_dollars": 93246.14, "num_transactions": 1029, "sale_dollars_m": 0.09324614, "tax_flag": "yes"}, {"county": "ADAMS", "sale_dollars": 44659.25, "num_transactions": 654, "sale_dollars_m": 0.04465925, "tax_flag": "no"}, {"county": "ALLAMAKEE", "sale_dollars": 288569.06, "num_transactions": 2382, "sale_dollars_m": 0.28856906, "tax_flag": "no"}, {"county": "APPANOOSE", "sale_dollars": 253574.57, "num_transactions": 1899, "sale_dollars_m": 0.25357457, "tax_flag": "yes"}, {"county": "AUDUBON", "sale_dollars": 84611.12, "num_transactions": 1063, "sale_dollars_m": 0.08461112, "tax_flag": "no"}, {"county": "BENTON", "sale_dollars": 375429.3, "num_transactions": 4071, "sale_dollars_m": 0.37542929999999997, "tax_flag": "no"}, {"county": "BLACK HAWK", "sale_dollars": 5497978.12, "num_transactions": 34244, "sale_dollars_m": 5.49797812, "tax_flag": "yes"}, {"county": "BOONE", "sale_dollars": 554843.96, "num_transactions": 4595, "sale_dollars_m": 0.5548439599999999, "tax_flag": "no"}, {"county": "BREMER", "sale_dollars": 628554.86, "num_transactions": 5002, "sale_dollars_m": 0.6285548599999999, "tax_flag": "yes"}, {"county": "BUCHANAN", "sale_dollars": 346335.72, "num_transactions": 3146, "sale_dollars_m": 0.34633571999999996, "tax_flag": "no"}, {"county": "BUENA VIST", "sale_dollars": 501097.9, "num_transactions": 4902, "sale_dollars_m": 0.5010979, "tax_flag": "no"}, {"county": "BUTLER", "sale_dollars": 108561.57, "num_transactions": 1150, "sale_dollars_m": 0.10856157000000001, "tax_flag": "no"}, {"county": "CALHOUN", "sale_dollars": 98259.84, "num_transactions": 788, "sale_dollars_m": 0.09825984, "tax_flag": "yes"}, {"county": "CARROLL", "sale_dollars": 916324.38, "num_transactions": 5166, "sale_dollars_m": 0.91632438, "tax_flag": "yes"}, {"county": "CASS", "sale_dollars": 447982.79, "num_transactions": 2978, "sale_dollars_m": 0.44798278999999996, "tax_flag": "no"}, {"county": "CEDAR", "sale_dollars": 187422.36000000002, "num_transactions": 2240, "sale_dollars_m": 0.18742236, "tax_flag": "no"}, {"county": "CERRO GORD", "sale_dollars": 2197027.24, "num_transactions": 13841, "sale_dollars_m": 2.19702724, "tax_flag": "no"}, {"county": "CHEROKEE", "sale_dollars": 194736.84, "num_transactions": 2100, "sale_dollars_m": 0.19473684, "tax_flag": "yes"}, {"county": "CHICKASAW", "sale_dollars": 158526.32, "num_transactions": 1094, "sale_dollars_m": 0.15852632, "tax_flag": "no"}, {"county": "CLARKE", "sale_dollars": 325167.77, "num_transactions": 1729, "sale_dollars_m": 0.32516777, "tax_flag": "no"}, {"county": "CLAY", "sale_dollars": 580730.46, "num_transactions": 4569, "sale_dollars_m": 0.58073046, "tax_flag": "no"}, {"county": "CLAYTON", "sale_dollars": 211702.19, "num_transactions": 2940, "sale_dollars_m": 0.21170219, "tax_flag": "no"}, {"county": "CLINTON", "sale_dollars": 1697173.86, "num_transactions": 9184, "sale_dollars_m": 1.6971738600000001, "tax_flag": "no"}, {"county": "CRAWFORD", "sale_dollars": 413734.17, "num_transactions": 2305, "sale_dollars_m": 0.41373416999999996, "tax_flag": "no"}, {"county": "DALLAS", "sale_dollars": 2929614.17, "num_transactions": 10511, "sale_dollars_m": 2.92961417, "tax_flag": "no"}, {"county": "DAVIS", "sale_dollars": 27815.94, "num_transactions": 423, "sale_dollars_m": 0.027815939999999997, "tax_flag": "no"}, {"county": "DECATUR", "sale_dollars": 36608.11, "num_transactions": 486, "sale_dollars_m": 0.03660811, "tax_flag": "no"}, {"county": "DELAWARE", "sale_dollars": 350363.61, "num_transactions": 2384, "sale_dollars_m": 0.35036361, "tax_flag": "no"}, {"county": "DES MOINES", "sale_dollars": 913572.68, "num_transactions": 7911, "sale_dollars_m": 0.91357268, "tax_flag": "no"}, {"county": "DICKINSON", "sale_dollars": 1575956.26, "num_transactions": 9497, "sale_dollars_m": 1.57595626, "tax_flag": "no"}, {"county": "DUBUQUE", "sale_dollars": 2945413.19, "num_transactions": 17228, "sale_dollars_m": 2.94541319, "tax_flag": "no"}, {"county": "EMMET", "sale_dollars": 210811.43, "num_transactions": 1841, "sale_dollars_m": 0.21081143, "tax_flag": "yes"}, {"county": "FAYETTE", "sale_dollars": 376570.2, "num_transactions": 3065, "sale_dollars_m": 0.3765702, "tax_flag": "no"}, {"county": "FLOYD", "sale_dollars": 378428.41, "num_transactions": 2588, "sale_dollars_m": 0.37842841, "tax_flag": "yes"}, {"county": "FRANKLIN", "sale_dollars": 171983.89, "num_transactions": 1740, "sale_dollars_m": 0.17198389, "tax_flag": "no"}, {"county": "FREMONT", "sale_dollars": 13774.06, "num_transactions": 143, "sale_dollars_m": 0.01377406, "tax_flag": "no"}, {"county": "GREENE", "sale_dollars": 226718.06, "num_transactions": 1507, "sale_dollars_m": 0.22671806, "tax_flag": "no"}, {"county": "GRUNDY", "sale_dollars": 61740.67, "num_transactions": 910, "sale_dollars_m": 0.06174067, "tax_flag": "yes"}, {"county": "GUTHRIE", "sale_dollars": 170031.57, "num_transactions": 1774, "sale_dollars_m": 0.17003157000000002, "tax_flag": "no"}, {"county": "HAMILTON", "sale_dollars": 242160.92, "num_transactions": 2726, "sale_dollars_m": 0.24216092, "tax_flag": "yes"}, {"county": "HANCOCK", "sale_dollars": 147944.4, "num_transactions": 1197, "sale_dollars_m": 0.1479444, "tax_flag": "no"}, {"county": "HARDIN", "sale_dollars": 613251.52, "num_transactions": 4242, "sale_dollars_m": 0.61325152, "tax_flag": "yes"}, {"county": "HARRISON", "sale_dollars": 198429.96, "num_transactions": 3460, "sale_dollars_m": 0.19842996, "tax_flag": "no"}, {"county": "HENRY", "sale_dollars": 414190.49, "num_transactions": 2986, "sale_dollars_m": 0.41419048999999997, "tax_flag": "yes"}, {"county": "HOWARD", "sale_dollars": 313848.87, "num_transactions": 2197, "sale_dollars_m": 0.31384887, "tax_flag": "no"}, {"county": "HUMBOLDT", "sale_dollars": 279384.34, "num_transactions": 1957, "sale_dollars_m": 0.27938434, "tax_flag": "yes"}, {"county": "IDA", "sale_dollars": 156807.5, "num_transactions": 1678, "sale_dollars_m": 0.1568075, "tax_flag": "no"}, {"county": "IOWA", "sale_dollars": 265273.06, "num_transactions": 3309, "sale_dollars_m": 0.26527306, "tax_flag": "no"}, {"county": "JACKSON", "sale_dollars": 343435.42, "num_transactions": 2216, "sale_dollars_m": 0.34343542, "tax_flag": "no"}, {"county": "JASPER", "sale_dollars": 625237.91, "num_transactions": 4972, "sale_dollars_m": 0.6252379100000001, "tax_flag": "no"}, {"county": "JEFFERSON", "sale_dollars": 406597.02999999997, "num_transactions": 2363, "sale_dollars_m": 0.40659702999999997, "tax_flag": "yes"}, {"county": "JOHNSON", "sale_dollars": 6229105.46, "num_transactions": 31333, "sale_dollars_m": 6.2291054599999995, "tax_flag": "no"}, {"county": "JONES", "sale_dollars": 414503.08, "num_transactions": 3571, "sale_dollars_m": 0.41450308, "tax_flag": "no"}, {"county": "KEOKUK", "sale_dollars": 68299.85, "num_transactions": 1733, "sale_dollars_m": 0.06829985000000001, "tax_flag": "no"}, {"county": "KOSSUTH", "sale_dollars": 707291.33, "num_transactions": 4244, "sale_dollars_m": 0.70729133, "tax_flag": "yes"}, {"county": "LEE", "sale_dollars": 1070335.02, "num_transactions": 7179, "sale_dollars_m": 1.07033502, "tax_flag": "no"}, {"county": "LINN", "sale_dollars": 9093390.299999999, "num_transactions": 52019, "sale_dollars_m": 9.0933903, "tax_flag": "no"}, {"county": "LOUISA", "sale_dollars": 52938.3, "num_transactions": 835, "sale_dollars_m": 0.0529383, "tax_flag": "no"}, {"county": "LUCAS", "sale_dollars": 165444.42, "num_transactions": 1622, "sale_dollars_m": 0.16544442, "tax_flag": "no"}, {"county": "LYON", "sale_dollars": 255880.45, "num_transactions": 2469, "sale_dollars_m": 0.25588045000000004, "tax_flag": "no"}, {"county": "MADISON", "sale_dollars": 268891.67, "num_transactions": 2482, "sale_dollars_m": 0.26889166999999997, "tax_flag": "no"}, {"county": "MAHASKA", "sale_dollars": 447556.2, "num_transactions": 4189, "sale_dollars_m": 0.4475562, "tax_flag": "no"}, {"county": "MARION", "sale_dollars": 865246.94, "num_transactions": 6199, "sale_dollars_m": 0.8652469399999999, "tax_flag": "no"}, {"county": "MARSHALL", "sale_dollars": 955966.1, "num_transactions": 7106, "sale_dollars_m": 0.9559660999999999, "tax_flag": "no"}, {"county": "MILLS", "sale_dollars": 180119.93, "num_transactions": 2154, "sale_dollars_m": 0.18011992999999998, "tax_flag": "no"}, {"county": "MITCHELL", "sale_dollars": 128357.58, "num_transactions": 1641, "sale_dollars_m": 0.12835758, "tax_flag": "no"}, {"county": "MONONA", "sale_dollars": 161116.13, "num_transactions": 2892, "sale_dollars_m": 0.16111613, "tax_flag": "no"}, {"county": "MONROE", "sale_dollars": 104447.4, "num_transactions": 799, "sale_dollars_m": 0.1044474, "tax_flag": "no"}, {"county": "MONTGOMERY", "sale_dollars": 211488.77, "num_transactions": 1599, "sale_dollars_m": 0.21148877, "tax_flag": "no"}, {"county": "MUSCATINE", "sale_dollars": 1036432.91, "num_transactions": 8394, "sale_dollars_m": 1.03643291, "tax_flag": "no"}, {"county": "OBRIEN", "sale_dollars": 444413.41, "num_transactions": 4255, "sale_dollars_m": 0.44441340999999995, "tax_flag": "no"}, {"county": "OSCEOLA", "sale_dollars": 76480.89, "num_transactions": 889, "sale_dollars_m": 0.07648089, "tax_flag": "no"}, {"county": "PAGE", "sale_dollars": 416675.52, "num_transactions": 3769, "sale_dollars_m": 0.41667552, "tax_flag": "no"}, {"county": "PALO ALTO", "sale_dollars": 265811.15, "num_transactions": 2895, "sale_dollars_m": 0.26581115, "tax_flag": "no"}, {"county": "PLYMOUTH", "sale_dollars": 589199.03, "num_transactions": 4139, "sale_dollars_m": 0.5891990300000001, "tax_flag": "no"}, {"county": "POCAHONTAS", "sale_dollars": 93554.4, "num_transactions": 1069, "sale_dollars_m": 0.0935544, "tax_flag": "no"}, {"county": "POLK", "sale_dollars": 23911443.029999997, "num_transactions": 116912, "sale_dollars_m": 23.911443029999997, "tax_flag": "yes"}, {"county": "POTTAWATTA", "sale_dollars": 3718199.09, "num_transactions": 18396, "sale_dollars_m": 3.7181990899999997, "tax_flag": "no"}, {"county": "POWESHIEK", "sale_dollars": 484846.69, "num_transactions": 5041, "sale_dollars_m": 0.48484669, "tax_flag": "no"}, {"county": "RINGGOLD", "sale_dollars": 47580.16, "num_transactions": 385, "sale_dollars_m": 0.04758016, "tax_flag": "no"}, {"county": "SAC", "sale_dollars": 208596.98, "num_transactions": 2316, "sale_dollars_m": 0.20859698000000002, "tax_flag": "no"}, {"county": "SCOTT", "sale_dollars": 7335268.59, "num_transactions": 34502, "sale_dollars_m": 7.33526859, "tax_flag": "no"}, {"county": "SHELBY", "sale_dollars": 297609.95, "num_transactions": 1745, "sale_dollars_m": 0.29760995, "tax_flag": "no"}, {"county": "SIOUX", "sale_dollars": 586566.53, "num_transactions": 4212, "sale_dollars_m": 0.58656653, "tax_flag": "no"}, {"county": "STORY", "sale_dollars": 3774317.63, "num_transactions": 20706, "sale_dollars_m": 3.77431763, "tax_flag": "yes"}, {"county": "TAMA", "sale_dollars": 187365.42, "num_transactions": 2065, "sale_dollars_m": 0.18736542, "tax_flag": "no"}, {"county": "TAYLOR", "sale_dollars": 41139.74, "num_transactions": 592, "sale_dollars_m": 0.04113974, "tax_flag": "yes"}, {"county": "UNION", "sale_dollars": 327594.21, "num_transactions": 2902, "sale_dollars_m": 0.32759421, "tax_flag": "no"}, {"county": "VAN BUREN", "sale_dollars": 51086.43, "num_transactions": 712, "sale_dollars_m": 0.05108643, "tax_flag": "no"}, {"county": "WAPELLO", "sale_dollars": 877545.08, "num_transactions": 6163, "sale_dollars_m": 0.87754508, "tax_flag": "no"}, {"county": "WARREN", "sale_dollars": 1030838.42, "num_transactions": 6461, "sale_dollars_m": 1.03083842, "tax_flag": "yes"}, {"county": "WASHINGTON", "sale_dollars": 628551.79, "num_transactions": 4523, "sale_dollars_m": 0.62855179, "tax_flag": "no"}, {"county": "WAYNE", "sale_dollars": 64046.17, "num_transactions": 542, "sale_dollars_m": 0.06404617, "tax_flag": "no"}, {"county": "WEBSTER", "sale_dollars": 1071520.82, "num_transactions": 6654, "sale_dollars_m": 1.0715208200000002, "tax_flag": "no"}, {"county": "WINNEBAGO", "sale_dollars": 289786.5, "num_transactions": 3160, "sale_dollars_m": 0.2897865, "tax_flag": "no"}, {"county": "WINNESHIEK", "sale_dollars": 501083.04, "num_transactions": 3175, "sale_dollars_m": 0.5010830399999999, "tax_flag": "no"}, {"county": "WOODBURY", "sale_dollars": 3502649.58, "num_transactions": 19632, "sale_dollars_m": 3.50264958, "tax_flag": "no"}, {"county": "WORTH", "sale_dollars": 116221.86, "num_transactions": 1081, "sale_dollars_m": 0.11622186, "tax_flag": "no"}, {"county": "WRIGHT", "sale_dollars": 182583.3, "num_transactions": 1620, "sale_dollars_m": 0.18258329999999998, "tax_flag": "no"}]}}, {"mode": "vega-lite"});
</script>



From this graph it's easy to see how sales break down for taxable counties, especially that Polk county, which has the most sales by far, is a taxable state.

To get more of the details I'll create and format a pivot table of the data.


```python
# create the pivot table, with count and sum of sales
pivot_df = county_sales_t12m.pivot_table(values=['sale_dollars', 'num_transactions'], index=['tax_flag'], aggfunc=['count', 'sum'], margins=True)
# remove redundant column
del pivot_df[('count', 'num_transactions')]
# calculate share of sales
pivot_df['sales_share'] = pivot_df['sum', 'sale_dollars'] / county_sales_t12m['sale_dollars'].sum()
pivot_df['trans_count_share'] = pivot_df['sum', 'num_transactions'] / county_sales_t12m['num_transactions'].sum()
# rename columns for easy data manipulation
pivot_df.columns = ['county_count', 'transaction_count', 'sum_sales', 'share_sales', 'trans_count_share']
# reorder columns
pivot_df = pivot_df[['county_count', 'transaction_count', 'trans_count_share', 'sum_sales', 'share_sales']]
# format the output to make it easier to read
format_dict = {'sum_sales':'${0:,.0f}', 'share_sales':'{:.2%}', 'transaction_count':'{:,}', 'trans_count_share':'{:.2%}'}
pivot_df.style.format(format_dict)
```




<style type="text/css">
</style>
<table id="T_64836_">
  <thead>
    <tr>
      <th class="blank level0" >&nbsp;</th>
      <th class="col_heading level0 col0" >county_count</th>
      <th class="col_heading level0 col1" >transaction_count</th>
      <th class="col_heading level0 col2" >trans_count_share</th>
      <th class="col_heading level0 col3" >sum_sales</th>
      <th class="col_heading level0 col4" >share_sales</th>
    </tr>
    <tr>
      <th class="index_name level0" >tax_flag</th>
      <th class="blank col0" >&nbsp;</th>
      <th class="blank col1" >&nbsp;</th>
      <th class="blank col2" >&nbsp;</th>
      <th class="blank col3" >&nbsp;</th>
      <th class="blank col4" >&nbsp;</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th id="T_64836_level0_row0" class="row_heading level0 row0" >no</th>
      <td id="T_64836_row0_col0" class="data row0 col0" >79</td>
      <td id="T_64836_row0_col1" class="data row0 col1" >412,599</td>
      <td id="T_64836_row0_col2" class="data row0 col2" >65.35%</td>
      <td id="T_64836_row0_col3" class="data row0 col3" >$64,387,331</td>
      <td id="T_64836_row0_col4" class="data row0 col4" >61.83%</td>
    </tr>
    <tr>
      <th id="T_64836_level0_row1" class="row_heading level0 row1" >yes</th>
      <td id="T_64836_row1_col0" class="data row1 col0" >20</td>
      <td id="T_64836_row1_col1" class="data row1 col1" >218,756</td>
      <td id="T_64836_row1_col2" class="data row1 col2" >34.65%</td>
      <td id="T_64836_row1_col3" class="data row1 col3" >$39,754,270</td>
      <td id="T_64836_row1_col4" class="data row1 col4" >38.17%</td>
    </tr>
    <tr>
      <th id="T_64836_level0_row2" class="row_heading level0 row2" >All</th>
      <td id="T_64836_row2_col0" class="data row2 col0" >99</td>
      <td id="T_64836_row2_col1" class="data row2 col1" >631,355</td>
      <td id="T_64836_row2_col2" class="data row2 col2" >100.00%</td>
      <td id="T_64836_row2_col3" class="data row2 col3" >$104,141,601</td>
      <td id="T_64836_row2_col4" class="data row2 col4" >100.00%</td>
    </tr>
  </tbody>
</table>




We can see that although only 20 out of 99 counties collect sales tax, they make up almost 38% of sales, and ~35% of the transaction count.

## Next Steps
Armed with the information I was able to uncover with this analysis of transactions, I can take the following next steps:<br>
1) Communicate the findings to the accounting group and make sure they understand exactly how I sourced the data so they can pressure test my approach. Then I can create a table in the cloud data warehouse that gets automatically populated with new transacional data on a daily basis (or whatever cadence is necessary for the task) so that it is easily accessible going forward. This creates a stable data source instead of a brittle, 'hacked-together' process.<br>
2) At the same time I can reach out to vendors that offer sales-tax engine solutions, get demos and compare the solutions against each other. Depending on the offerings and costs, a cost-benefit analysis can be done to evaluate whether it makes more sense to use a vendor or whether it is worth the effort to build our own tax-engine solution.
