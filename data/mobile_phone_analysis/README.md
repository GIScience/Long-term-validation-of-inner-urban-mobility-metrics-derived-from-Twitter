# Mobile phone data analysis

## Structure

This subdirectory contains example code for mobile phone data analysis and is structured as follows:

* `1_Retrieval_of_individual_human_movement_trajectories_and_collective_OD_matrices.ipynb`: This notebook provides a glimpse of how PySpark can be used to retrieve individual human movement trajectories and collective OD matrices from raw mobile phone records.
* `2_Exploration_and_analysis_of_mobility_patterns.ipynb`: This notebook aims to explore and analyze mobility patterns derived from mobile phone records, as calculated in the notebook `1_Retrieval_of_individual_human_movement_trajectories_and_collective_OD_matrices`.
* `3_Visualization_Figure_5.ipynb`: This notebook was used for visualizing Figure 5.
* `4_Visualization_Appendix_A1.ipynb`: This notebook was used for visualizing Appendix A1.
* `Optional__Bash_command_for_loop.py`: Python script for running the notebook 1_Retrieval_of_individual_human_movement_trajectories_and_collective_OD_matrices in a loop.
* `Optional__Explore_distribution_of_inter-event_time_to_select_threshold_in_notebook_loop.ipynb`: This notebook can help explore the distribution of the inter-event time between two antenna connections of a unique user.
* `Optional__Generate_synthetic_calldata.ipynb`: This notebook can help generate synthetic call data with the same data structure used for this analysis.
* `Optional__Retrieval_of_timestamps_from_calldata_filenames_to_run_analysis_in_a_loop.ipynb`: This notebook can help create a `data/0_input_data/timestamps/timestamps_df.csv` file, which is required as input when running `Optional__Bash_command_for_loop.py`.

## Data availability:
* Mobile phone records and corresponding products cannot be shared under a free license. However, the work can be reproduced with similar data that you may place in the `data/0_input_data/calldata` subdirectory. If you do not have access to mobile phone records, you can generate synthetic data using the `Optional__Generate_synthetic_calldata.ipynb` notebook
* The file `data/mobile_phone_analysis/2_final_output/daily_mobility_matrics_from_mobile_phone_data.csv`, which contains calculated mobility metrics from mobile phone data, is equivalent to the file `data/statistics/mobility_metrics_paper.csv` used for further plotting.

