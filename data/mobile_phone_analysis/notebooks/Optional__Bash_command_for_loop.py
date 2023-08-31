# Bash command to loop over time intervals of analysis
while IFS=, read -r field1 field2 field3
    do
        nbclick 1_Retrieval_of_individual_human_movement_trajectories_and_collective_OD_matrices.ipynb --start_timestamp="$field2" --end_timestamp="$field3" 
    done < timestamps_df.csv