import streamlit as st
import pandas as pd
import plotly.express as px
from Data_Processing.db_to_df import execute_query

# # Set page layout to wide
st.set_page_config(layout="wide")

def geo_visualization():
    # # Set page layout to wide
    # st.set_page_config(layout="wide")
    st.markdown("<h1 style='text-align: center; font-weight: bold;'>Phonepe-Geo Visualization Daashboard</h1>", unsafe_allow_html=True)

    # Side-by-side layout for user selection dropdowns and map
    col1, col2, col3 = st.columns([0.5, 2, 1])

    # Drop down for type of transaction 
    with col1:
        st.write("User Selection")
        type=st.selectbox("Select Type",['Transaction','User'])

    # Dropdown for selecting year/quarter
    with col1:
        year = st.selectbox('Select Year', list(range(2018, 2024)))
        quarter_mapping = {'Q1': 1, 'Q2': 2, 'Q3': 3, 'Q4': 4}
        quarter = st.selectbox('Select Quarter', ['Q1', 'Q2', 'Q3', 'Q4'], format_func=lambda x: quarter_mapping[x])

    # Load the India states GeoJSON data
    geojson_url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"


    # Display the map based on the user selection 
    if type=='Transaction':
        with col2:
            # st.write("Transaction Data across various Indian states")
            st.markdown("<h3 style='text-align: center'>Transaction Data across various Indian states</h3>", unsafe_allow_html=True)
            # Read the aggregated transaction data
            query = f"""select State,
                sum(Transacion_amount) as "Total_Transaction_Amount",
                sum(Transacion_count) as "Total_Transaction_Count"
            from phonepe.agg_trans 
            where Year={year}
            and Quarter={quarter_mapping[quarter]}
            group by State"""

            df_final = execute_query(query)
                    
            # Create the choropleth plot
            fig = px.choropleth(
                df_final,
                geojson=geojson_url,
                featureidkey='properties.ST_NM',
                locations='State',
                # locationmode='geojson-id', 
                # color='Total_Transaction_Amount',
                color='State',
                color_continuous_scale='Reds',
                hover_name='State',  # Display the state name when hovering
                hover_data={'Total_Transaction_Amount': ':,.2f','Total_Transaction_Count': ':,.2f'}  # Format transaction amount with comma and two decimal places
            )

            # Update layout to customize hover label
            fig.update_layout(
                hoverlabel=dict(bgcolor="white", font_size=16, font_family="Rockwell"),
                geo=dict(
                    scope='asia',
                    center={'lat': 20, 'lon': 77},
                    projection_scale=6
                )
            )

            # Update geos to fit bounds
            fig.update_geos(fitbounds="locations", visible=False)


            # Show the plot
            st.plotly_chart(fig)

        with col3:
            ## Logic to get the Categories information for a chosen Transaction, year, quarter combination 

            query_category=f"""select Transacion_type,
                                sum(Transacion_count) as Total_Transaction_count
                                from phonepe.agg_trans
                                where year = {year}
                                and Quarter = {quarter_mapping[quarter]}
                                group by Transacion_type;
                            """
            try:
                df_category=execute_query(query_category)
                if not df_category.empty:
                    st.write("Categories")
                    st.write(df_category)
                else:
                    st.write("No categories found.")
            except Exception as e:
                st.error(f"Error fetching categories: {e}")
            
            ## Logic to get the top states, districts, pincodes information for a chosen Transaction, year, quarter combination based on transaction count 

            top_queries={'state':f"""
                                    select State,
                                    sum(Transacion_count) as "Total_Transaction_count"
                                    from phonepe.agg_trans
                                    where Year={year}
                                    and Quarter={quarter_mapping[quarter]}
                                    group by State
                                    order by Total_Transaction_count DESC
                                    LIMIT 10
                                    """,
                        'district':f"""
                                    select district_name, total_transactions_count 
                                    from phonepe.top_trans_dist
                                    where year={year}
                                    and quarter={quarter_mapping[quarter]}
                                    order by total_transactions_count DESC
                                    limit 10
                                    """,
                        'pincode':f"""
                                    select pincode, total_transactions_count
                                    from phonepe.top_trans_pincode
                                    where year={year}
                                    and quarter={quarter_mapping[quarter]}
                                    order by total_transactions_count DESC
                                    limit 10;
                                    """
                        }
            df_top_tran_state=execute_query(top_queries['state'])
            df_top_tran_dist=execute_query(top_queries['district'])
            df_top_tran_pincode=execute_query(top_queries['pincode'])
            
            # Create tabs for top states, districts, and pincodes
            # with st.expander("Top 10 States, Districts, Pincodes"):
            tabs = st.tabs(["Top States", "Top Districts", "Top Pincodes"])
            with tabs[0]:
                st.write(df_top_tran_state)
            with tabs[1]:
                st.write(df_top_tran_dist)
            with tabs[2]:
                st.write(df_top_tran_pincode)

    elif type=='User':
        with col2:
            # st.write("User Data across various Indian states")
            st.markdown("<h4 style='text-align: center'>User Data across various Indian states</h4>", unsafe_allow_html=True)

            # Read the map user table
            query=f"""select State,
                    sum(number_of_reg_users) as "Registered_Users",
                    sum(total_app_opens) as "App_Opens"
                    from phonepe.map_user 
                    where Year={year}
                    and Quarter={quarter_mapping[quarter]}
                    group by State"""

            df_final = execute_query(query)

            # Create the choropleth plot
            fig = px.choropleth(
                df_final,
                geojson=geojson_url,
                featureidkey='properties.ST_NM',
                locations='State',
                # locationmode='geojson-id', 
                # color='Total_Transaction_Amount',
                color='State',
                color_continuous_scale='Reds',
                hover_name='State',  # Display the state name when hovering
                hover_data={'Registered_Users': ':,.2f','App_Opens': ':,.2f'},  # Format transaction amount with comma and two decimal places
            )

            # Update layout to customize hover label
            fig.update_layout(
                hoverlabel=dict(bgcolor="white", font_size=16, font_family="Rockwell"),
                geo=dict(
                    scope='asia',
                    center={'lat': 20, 'lon': 77},
                    projection_scale=6
                )
            )

            # Update geos to fit bounds
            fig.update_geos(fitbounds="locations", visible=False)


            # Show the plot
            st.plotly_chart(fig)
        
        with col3:
            ## Logic to get the top states, districts, pincodes information for a chosen Transaction, year, quarter combination based on transaction count 

            top_queries_user={'state':f"""
                                        select State,
                                               sum(number_of_reg_users) as "Registered_Users_Count"
                                               from phonepe.map_user
                                               where Year={year}
                                               and Quarter={quarter_mapping[quarter]}
                                               group by State
                                               order by Registered_Users_Count DESC
                                               LIMIT 10;
                                    """,
                        'district':f"""
                                    select district_name, registered_users_count 
                                    from phonepe.top_usr_dist
                                    where year={year}
                                    and Quarter={quarter_mapping[quarter]}
                                    order by registered_users_count desc
                                    LIMIT 10;
                                    """,
                        'pincode':f"""
                                    select pincode, registered_users_count 
                                    from phonepe.top_usr_pincode
                                    where year={year}
                                    and Quarter={quarter_mapping[quarter]}
                                    order by registered_users_count desc
                                    LIMIT 10;
                                    """
                        }
            df_top_usr_state=execute_query(top_queries_user['state'])
            df_top_usr_dist=execute_query(top_queries_user['district'])
            df_top_usr_pincode=execute_query(top_queries_user['pincode'])
            
            # Create tabs for top states, districts, and pincodes
            # with st.expander("Top 10 States, Districts, Pincodes"):
            tabs = st.tabs(["Top States", "Top Districts", "Top Pincodes"])
            with tabs[0]:
                st.write(df_top_usr_state)
            with tabs[1]:
                st.write(df_top_usr_dist)
            with tabs[2]:
                st.write(df_top_usr_pincode)

def additional_insights():
    st.markdown("<h1 style='text-align: center; font-weight: bold;'>Additional Insights on Phonepe data</h1>", unsafe_allow_html=True)
    insight_ques={"How has Phonepe performed over the years w.r.t transaction_count and transaction_amount?":"""select year, 
                                                                                                                       sum(Transacion_count) as "Transaction_count",
                                                                                                                       sum(Transacion_amount) as "Transaction_amount"
                                                                                                                from agg_trans
                                                                                                                group by year
                                                                                                                order by Transaction_count DESC """, 
                  "Top 10 states which generated most number of transactions/ Transaction Amount ?":["""select State,
                                                                                                         sum(Transacion_count) "Total_Transactions_count"
                                                                                                         from agg_trans
                                                                                                         group by State
                                                                                                         order by Total_Transactions_count DESC
                                                                                                         LIMIT 10""",
                                                                                                     """select State,
	                                                                                                     sum(Transacion_amount) "Total_Transaction_amount"
                                                                                                         from agg_trans
                                                                                                         group by State
                                                                                                         order by Total_Transaction_amount DESC
                                                                                                         LIMIT 10"""
                                                                                                    ],
                  "10 Least Performing States w.r.t transaction_count and transaction_amount":["""select State,
                                                                                                   sum(Transacion_count) "Total_Transactions_count"
                                                                                                   from agg_trans
                                                                                                   group by State
                                                                                                   order by Total_Transactions_count ASC
                                                                                                   LIMIT 10""",
                                                                                                """select State,
                                                                                                    sum(Transacion_amount) "Total_Transaction_amount"
                                                                                                    from agg_trans
                                                                                                    group by State
                                                                                                    order by Total_Transaction_amount ASC
                                                                                                    LIMIT 10;"""
                                                                                             ],
                  "In each year which Quarter has highest Transaction count":"""SELECT year, quarter, Total_Transaction_count
                                                                                FROM (
                                                                                    SELECT year,
                                                                                        quarter,
                                                                                        SUM(Transacion_count) AS Total_Transaction_count,
                                                                                        ROW_NUMBER() OVER (PARTITION BY year ORDER BY SUM(Transacion_count) DESC) AS rn
                                                                                    FROM agg_trans
                                                                                    GROUP BY year, quarter
                                                                                ) AS A
                                                                                WHERE rn = 1;""",
                  "In each year which Quarter has highest Transaction amount":"""SELECT year, quarter, Total_Transaction_amount
                                                                                    FROM (
                                                                                        SELECT year,
                                                                                            quarter,
                                                                                            SUM(Transacion_amount) AS Total_Transaction_amount,
                                                                                            ROW_NUMBER() OVER (PARTITION BY year ORDER BY SUM(Transacion_amount) DESC) AS rn
                                                                                        FROM agg_trans
                                                                                        GROUP BY year, quarter
                                                                                    ) AS A
                                                                                    WHERE rn = 1; """,
                  "Over the years has phonepe user base increased or decreased ?":"""select Year,
                                                                                    sum(user_count) "User_Count"
                                                                                    from agg_user
                                                                                    group by Year
                                                                                    order by User_Count DESC;""",
                  "Top 10/ Least 10 mobile brand customers using the Phonepe application the most ?":["""select user_brand,
                                                                                                        sum(user_count) "user_count"
                                                                                                        from agg_user
                                                                                                        group by user_brand
                                                                                                        order by user_count desc
                                                                                                        limit 10; """,
                                                                                                        """select user_brand,
                                                                                                            sum(user_count) "user_count"
                                                                                                            from agg_user
                                                                                                            group by user_brand
                                                                                                            order by user_count asc
                                                                                                            limit 10; """],
                  "Top 10/Least 10 states w.r.t user_counts ":["""select State,
                                                                    sum(reg_users) "user_count"
                                                                    from agg_user_app_open
                                                                    group by State 
                                                                    Order by user_count DESC
                                                                    LIMIT 10; """,
                                                                """select State,
                                                                            sum(reg_users) "user_count"
                                                                            from agg_user_app_open
                                                                            group by State 
                                                                            Order by user_count ASC
                                                                            LIMIT 10;"""],
                  "User_count Vs Revenue. If the user_count in a state is more does it mean high revenue ? ":"""select T.State, 
                                                                                                                T.Total_Transaction,
                                                                                                                U.Total_Reg_Users
                                                                                                            from (select State, 
                                                                                                                        sum(Transacion_amount) "Total_Transaction" 
                                                                                                                        from agg_trans
                                                                                                                group by State) T
                                                                                                            join (select State, 
                                                                                                                sum(reg_users) "Total_Reg_Users",
                                                                                                                sum(app_opens) "Total_App_Opens"
                                                                                                                from agg_user_app_open
                                                                                                                group by State) U
                                                                                                            on T.State=U.State;""",
                  "Total App Opens Vs Revenue. Is there any relationship between App Opens and Revenue":"""select T.State, 
                                                                                                                T.Total_Transaction,
                                                                                                                U.Total_App_Opens
                                                                                                            from (select State, 
                                                                                                                        sum(Transacion_amount) "Total_Transaction" 
                                                                                                                        from agg_trans
                                                                                                                group by State) T
                                                                                                            join (select State, 
                                                                                                                sum(reg_users) "Total_Reg_Users",
                                                                                                                sum(app_opens) "Total_App_Opens"
                                                                                                                from agg_user_app_open
                                                                                                                group by State) U
                                                                                                            on T.State=U.State;"""}
    question=st.selectbox("Select a question:", list(insight_ques.keys()))
    if st.button("Get Answer"):
        if question in insight_ques:
            if type(insight_ques[question])!=list:
                try:
                    query_result=execute_query(insight_ques[question])
                    if query_result is not None:
                        st.write("Result of the Question")
                        if 'year' in query_result.columns:
                            result_without_formatting = query_result.copy()
                            result_without_formatting['year'] = result_without_formatting['year'].astype(str)
                            st.write(result_without_formatting)
                        elif 'Year' in query_result.columns:
                            result_without_formatting = query_result.copy()
                            result_without_formatting['Year'] = result_without_formatting['Year'].astype(str)
                            st.write(result_without_formatting)
                        else:
                            st.write(query_result)
                        visualize_data(question,result_without_formatting)
                    else:
                        st.error('Error Fetching the data from database')
                except Exception as e:
                    st.error(f"Error executing the query : {e}")
            else:
                for items in insight_ques[question]:
                    # st.write("The SQL query is :",items)
                    try:
                        result=execute_query(items)
                        try:
                            if result is not None:
                                st.write("Result of the Question")
                                if 'year' in result.columns:
                                    result_without_formatting = result.copy()
                                    result_without_formatting['year'] = result_without_formatting['year'].astype(str)
                                    st.write(result_without_formatting)
                                elif 'Year' in result.columns:
                                    result_without_formatting = result.copy()
                                    result_without_formatting['Year'] = result_without_formatting['Year'].astype(str)
                                    st.write(result_without_formatting)
                                else:
                                    st.write(result)
                            else:
                                st.error('error Fetching the data from database')
                            visualize_data(question,result)
                        except Exception as e:
                            st.error(f"Error executing query : {e}")
                    except Exception as e:
                        st.error(f"Error executing the query : {e}")


def visualize_data(question,df):
    if question=='How has Phonepe performed over the years w.r.t transaction_count and transaction_amount?':
        # Convert 'Year' column to string
        # df['year'] = df['year'].astype(str)
        # Remove commas and convert 'Transaction_count' and 'Transaction_amount' to integer
        df['Transaction_count'] = df['Transaction_count'].apply(lambda x: int(str(x).replace(',', '')))
        df['Transaction_amount'] = df['Transaction_amount'].apply(lambda x: int(str(x).replace(',', '')))

        # st.write(df)
        # print(df.dtypes)

        # Plotly bar chart
        fig = px.bar(df, x='year', y=['Transaction_count', 'Transaction_amount'],
                    labels={'value': 'Amount', 'variable': 'Metric'},
                    title='Transaction Count and Transaction Amount Over the Years')

        # Customize layout
        fig.update_layout(xaxis_title='year', yaxis_title='Amount')

        # Display the plot in Streamlit
        st.plotly_chart(fig)
    
    elif question=="Top 10 states which generated most number of transactions/ Transaction Amount ?":
    #    print(df.dtypes)
       if "Total_Transactions_count" in df.columns:
        #    print("Inside the if statement")
           df['Total_Transactions_count'] = df['Total_Transactions_count'].apply(lambda x: int(str(x).replace(',', '')))
        #    print(df)
           # Plotly bar chart
           fig = px.bar(df, x='State', y='Total_Transactions_count',
                        labels={'Total_Transactions_count': 'Total Transactions Count'},
                        title='Top 10 states w.r.t no. of Transactions')

           # Customize layout
           fig.update_layout(xaxis_title='State', yaxis_title='Total Transaction Count')

           # Display the plot in Streamlit
           st.plotly_chart(fig)    
       elif "Total_Transaction_amount" in df.columns:
           print("inside elif statement")
           df['Total_Transaction_amount'] = df['Total_Transaction_amount'].apply(lambda x: int(str(x).replace(',', '')))
           # Plotly bar chart
           fig = px.bar(df, x='State', y='Total_Transaction_amount',
                        labels={'Total_Transaction_amount': 'Total Transactions Amount'},
                        title='Top 10 states w.r.t Transaction value')

           # Customize layout
           fig.update_layout(xaxis_title='State', yaxis_title='Total Transaction Amount')

           # Display the plot in Streamlit
           st.plotly_chart(fig)    

        




# Add sidebar options

option = st.sidebar.selectbox(
    'Choose an option:',
    ('Geo Visualization Dashboard', 'Additional Insights on Phonepe data')
)

# Based on the selected option, call the corresponding function
if option == 'Geo Visualization Dashboard':
    geo_visualization()
elif option == 'Additional Insights on Phonepe data':
    additional_insights()


