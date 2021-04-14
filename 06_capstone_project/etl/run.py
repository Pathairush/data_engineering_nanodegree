import extract_fact_label, stage_fact, load_fact, load_dim_user, load_dim_date, load_dim_state, load_dim_country, data_quality_check

if __name__ == "__main__":
    
    # extract mapping table for label description file
    extract_fact_label.main()
    
    # staging I94 immigration fact table
    stage_fact.main()
    
    # load and transform data from souce to delta format
    load_fact.main()
    load_dim_user.main()
    load_dim_date.main()
    load_dim_state.main()
    load_dim_country.main()
    
    # run data quality check
    data_quality_check.main()