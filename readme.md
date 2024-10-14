# Long-term validation of inner-urban mobility metrics derived from Twitter

[![DOI](https://zenodo.org/badge/679714419.svg)](https://zenodo.org/badge/latestdoi/679714419)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Abstract
Urban mobility analysis using Twitter as a proxy has gained significant attention in various application fields; however, long-term validation studies are scarce. This paper addresses this gap by assessing the reliability of Twitter data for modeling inner-urban mobility dynamics over a 27-month period in the metropolitan area of Rio de Janeiro, Brazil. The evaluation involves the validation of Twitter-derived mobility estimates at both temporal and spatial scales, employing over 160 billion mobile phone records of around three million users during the non-stationary mobility period from April 2020 to June 2022, which coincided with the COVID-19 pandemic. The results highlight the need for caution when using Twitter for short-term modeling of urban mobility flows. Short-term inference can be influenced by Twitter policy changes and the availability of publicly accessible tweets. On the other hand, this long-term study demonstrates that employing multiple mobility metrics simultaneously, analyzing dynamic and static mobility changes concurrently, and employing robust preprocessing techniques such as rolling window downsampling can enhance the inference capabilities of Twitter data. These novel insights gained from a long-term perspective are vital, as Twitter — rebranded to X in 2023 — is extensively used by researchers worldwide to infer human movement patterns. Since conclusions drawn from studies using Twitter could be used to inform public policy, emergency response, and urban planning, evaluating the reliability of this data is of utmost importance.

# Manuscript
The peer-reviewed manuscript, published in "Environment and Planning B: Urban Analytics and City Science", is available online at https://doi.org/10.1177/23998083241278275.


# Setup
## Environment
The file twitter_long_term_validation.yml provides the packages needed for installing the environment for this repository.

## Data and Privacy
Our study is based on twitter and mobile phone data. Since twitter data cannot be published, this repository does not include the actual base data of our study. We just publish the tweet ids. Mobile phone records could also not be shared. Relevant scripts for processing such data however can be found in the 'data/mobile_phone_analysis` subdirectory.

The first notebook handles the requests to the twitter API, and the second notebook preprocesses this data. This preprocessing is of course only possible with respective data. This is why we created a list of all tweet ids used in this analysis which is saved as a csv-file: data/tweets/all_tweet_ids.csv

The raw answer form twitter (a json structure) is saved in the first notebook in: data/tweets/retrieved_tweets.txt
The preprocessed tweets are then saved in data/tweets/preprocessed_tweets_with_poi_location.csv.

For both of these files we created a dummy dataset which shows the structure of each file. The dummy_retrieved_tweets.txt contains five example tweets with sensitive user information removed, the dummy_preprocessed_tweets_with_poi_location.csv contains five example preprocessed tweets.

## Structure
The code for the Study was structured in multiple jupyter notebooks. These can be summarized as follows: 

The notebooks starting with A are preprocessing steps, from data collection and preprocessing to first explorations. 

The B notebooks constitute the calculations of the actual mobility variables. For our actual research, these B notebooks are replaced by the "notebooks_loops_script".py" due to processing times. Since calculations have to be made for each rolling window individually, using notebooks is not efficient due to loading times. However, the notebooks provide a good overview on what is happening in the "notebooks_loops_script". The results of this loop are written in the "data/statistics_..._overlap.csv" files. For the notebooks the files are always marked with a "notebooksdemo" in their filename. The overlap in the name specifies that the rolling windows overlap each other.

The C notebook handles data post-processing.

And finally, the D notebook visualizes and present our findings.

Additional Information:
- In the code "neighborhoods" as in the 163 districts of Rio de Janeiro are often referred  to as "barrios". This is the same thing.

## A: Data collection, preprocessing and inspection

### A_01 - Data collection
The first notebook clearly outlines the requests sent to the twitter API and characterized shows the way of obtaining the studies data. For that, an extensive python script "get_tweets.py" is used that can be found in the folder backend_codes. This is a program that was developed for another project to communicate with the twitter API.

### A_02 - Data preprocessing
The obtained data is then preprocessed in the second notebook. The tweets are filtered and clipped to the relevant study areas. Then they are matched to the 163 neighborhoods of Rio de Janeiro.

### A_03 - Data Inspection
Here, the preprocessed data is inspected, some interesting characteristics are already used in the paper.

## B: Calculating the metrics (loop)
As mentioned above, the whole B-section is replaced by the notebook_loop_script.py. this script is also documented can be run, however a full processing with the normal amount of tweets takes a very long time.
Since the same metrics have to be calculated many times, a script is more efficient here. The notebooks B_01 to B_05 still provide a overview of what happens in the script.

### B_01 Metadata
The first set of variables derived for each individual rolling window is metadata. Simple values like the number of tweets or users are calculated here.

The calculated metrics here are:
- Number of tweets
- Number of unique users
- Median / mean tweets per user
- Number of users with more than on tweet / location / neighborhood

### B_02 OD matrices
This notebook constructs OD Matrices for each rolling window and saves them in a specific folder structure.
No metrics are calculated in this notebook.

### B_03 Mobility metrics on user dicts
For each user, a sequence of locations is constructed using the skmob library. Using these sequences, a multitude of mobility metrics are derived. Not all of these are analyzed in the further study.

The calculated metrics here are:
- Number of total trips
- __Mean / Std radius of gyration__
- __Mean / Std jump lengths (distances between trips), each user weighted the same__
- Mean /Std jump lengths, without users that only sent tweets from the same location
- Mean / Std jump lengths, each trip weighted the same
- Mean / Std number of visited locations per user
- Mean / Std maximum distance from home per user
- Mean random location entropy based on neighborhoods (not on pois)

### B_04 Mobility metrics based on OD-matrices
This notebook loads an OD-matrix, calculated in Notebook B_02 and uses it to create new statistics.

The calculated metrics are:
- __Number of real movements__ (every movement that was registered in the OD-matrix, lower than number of total trips since trips to the same location are not counted)
- __Graph modularity__ (for measuring network variability, especially interesting to see lockdown patterns)
- inflow / outflow values for each neighborhood
- mean number of incoming / outgoing meaningful connections (more than 5 % of trip volume). (For each neighborhood, the connected neighborhoods are counted that constitute at least 5 % of incoming / outgoing trips. Of these 163 values, the mean is taken)
- mean distance to strongest inflow / outflow connection

### B_05 Landuse metrics
This notebook analyses, from which landuse areas tweets were sent from.

The calculated metrics are:
- relative tweet counts for each landuse class. In the study we used __"relative tweets in residential areas"__

## C: Post-processing
### C_01 Post processing
This notebook handles the post-processing of the data, before plots are created. First, OD-matrices of twitter and mobile phone data are loaded, normalized and exported again.

Then the twitter metrics from the B-notebooks are loaded as well as the metrics from the mobile phone data. For the paper, five metrics were analyzed:
- Number of movements
- Jump lengths (mean distance between tweets)
- Radius of gyration
- Graph modularity
- Relative tweets in residential areas

In order to ensure comparability, the metrics are processed in two/three ways. 

(First, normalization: After first inspection, the data revealed a suspicious dependence on tweet volume, so the metrics were normalized by dividing (in case of number of movements, jump lengths and radius of gyration), multiplying (graph modularity, since it is inversely correlated) and doing nothing (relative tweets, no dependent on tweet amount). However, after further analysis, we could not find enough evidence to justify a normalization for our final analysis. The values in the study are therefore without this normalization)

Moving average: Since the data is very jittery, especially for smaller rolling window sizes, but also for bigger ones, we decided to put a 28-day rolling average on the data, to get more insight into the long-term patters of the metrics that this study tries to capture.

Scaling: The data was scaled between 0 and 1 (using x = (min / (max - min))) to ensure comparability between different window sizes, different metrics and between twitter and mobile phone data.

Also, the values from the sample-based dataset were processed in the same way.

The results are timeseries of each metric in the same range in the same time period for twitter and mobile phone data.

## D: Plotting
For plotting, "trend" refers to rolling average.

### D_01 Spatial plotting
First, the post-processed metrics (twitter and mobile phone data) and the neighborhood shapefile are loaded.

A correlation plots is drawn between the od-matrices of twitter and mobile phone data.

Furthermore, tweets per neighborhood are shown in a simple map.

Finally, a scatterplot for each neighborhoods population and number of tweets is plotted.

### D_02 Temporal plotting
This notebook produces most of the plots in the final paper. First, data is loaded. Then, the rolling window sizes 1, 7 and 15 are plotted for each of the five chosen metrics. One time with a rolling average and one time without, which are referenced in the appendix of the paper.

A plot comparing the metrics with and without a moving average is plotted.

The daily tweet amount and daily tweets per user are plotted.

For the correlation tables the a Pearson's r values is calculated for each rolling window size. Only days where twitter and mobile phone data have values are used. The table is exported to LATEX. Also plot, visualizing the development of the correlations by window size is shown.

The rolling window size with the highest correlation is the used to plot a comparison between twitter and mobile phone data for each metric.

Then, the moving window synchrony is plotted. Finally, the multi-boxplot is created.

### D_03 Absolute values twitter
In the end it was decided to put the absolute values of our metrics in the appendix as well. These are plotted here.
