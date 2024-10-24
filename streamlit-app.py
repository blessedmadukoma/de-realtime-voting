import json
import time
from kafka import KafkaConsumer
import pandas as pd
import streamlit as st
from env import connect_db
import matplotlib.pyplot as plt
import numpy as np
from streamlit_autorefresh import st_autorefresh


def create_kafka_consumer(topic_name):
    """create a kafka consumer"""

    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    return consumer


# @st.cache_data  # cache the result of this function
def fetch_voting_stats():
    conn = connect_db()

    cur = conn.cursor()

    # fetch total number of voters
    cur.execute("SELECT COUNT(*) FROM voters")
    voters_count = cur.fetchone()[0]

    # fetch total number of candidates
    cur.execute("SELECT COUNT(*) FROM candidates")
    candidates_count = cur.fetchone()[0]

    conn.close()

    return voters_count, candidates_count


def fetch_data_from_kafka(consumer: KafkaConsumer) -> list:
    messages: dict = consumer.poll(timeout_ms=1000)
    data = []

    for message in messages.values():
        for sub_message in message:
            print(sub_message.value)
            data.append(sub_message.value)

    return data


def plot_bar_chart(results):
    # data_type = results['candidate_name']
    data_type = [str(name) for name in results['candidate_name']]

    colors = plt.cm.viridis(np.linspace(0, 1, len(data_type)))

    plt.bar(data_type, results['total_votes'], color=colors)
    plt.xlabel('Candidates')
    plt.ylabel('Total Votes')
    plt.title('Total Votes per Candidate')
    plt.xticks(rotation=45)

    return plt


# def plot_donut_chart(results):
#     labels = list(results['candidate_name'])
#     sizes = list(results['total_votes'])

#     fig, ax = plt.subplots()
#     ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140)
#     ax.axis('equal')
#     plt.title('Total Votes per Candidate')

#     return fig

def plot_donut_chart(results):
    labels = list(results['candidate_name'])
    sizes = list(results['total_votes'])

    fig, ax = plt.subplots()

    # Create a donut chart with a hole in the middle for a "donut" appearance
    wedges, texts, autotexts = ax.pie(
        sizes, labels=labels, autopct='%1.1f%%', startangle=140, wedgeprops=dict(width=0.3)
    )

    # Adjust the position of the labels to avoid overlap
    for text in texts:
        text.set_fontsize(10)  # Adjust font size as needed
    for autotext in autotexts:
        autotext.set_fontsize(8)

    # Equal aspect ratio ensures the pie is drawn as a circle.
    ax.axis('equal')
    plt.title('Total Votes per Candidate')

    return fig


@st.cache_data(show_spinner=False)
def split_frame(frame, batch_size_rows):
    dataframe = [frame.loc[i:i + batch_size_rows - 1, :]
                 for i in range(0, len(frame), batch_size_rows)]

    return dataframe


def paginate_table(table_data):
    top_menu = st.columns(3)
    with top_menu[0]:
        sort = st.radio("Sort Data", options=[
                        "Yes", "No"], horizontal=1, index=1)

    if sort == "Yes":
        with top_menu[1]:
            sort_field = st.selectbox("Sort By", options=table_data.columns)
        with top_menu[2]:
            sort_direction = st.radio("Sort Direction", options=[
                                      "Asc ⬆️", "Desc ⬇️"], horizontal=True)

        table_data = table_data.sort_values(
            by=sort_field, ascending=sort_direction == "Asc ⬆️", ignore_index=True)

    pagination = st.container()

    bottom_menu = st.columns((4, 1, 1))

    with bottom_menu[2]:
        batch_size = st.selectbox("Page Size", options=[10, 25, 50, 100])
    with bottom_menu[1]:
        total_pages = (
            int((len(table_data) / batch_size)
                if int(len(table_data) / batch_size) > 0 else 1)
        )

        current_page = st.number_input(
            "Page", min_value=1, max_value=total_pages, step=1
        )

    with bottom_menu[0]:
        st.markdown(f"Page **{current_page}** of **{total_pages}**")

    pages = split_frame(table_data, batch_size)
    pagination.dataframe(
        data=pages[current_page - 1], use_container_width=True)


def side_bar():
    if st.session_state.get('latest_update') is None:
        st.session_state.latest_update = time.time()

    refresh_interval = st.sidebar.slider(
        "Refresh Interval (seconds)", 5, 60, 10)
    st_autorefresh(interval=refresh_interval * 1000, key="auto_refresh")

    if st.sidebar.button("Refresh Data"):
        st.session_state.latest_update = time.time()


def update_data():
    last_refresh = st.empty()
    last_refresh.text(f"Last refresh at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # fetch voting statistics from postgres
    voters_count, candidates_count = fetch_voting_stats()

    # display statistics
    st.markdown("""---""")
    col1, col2 = st.columns(2)
    col1.metric("Total Voters", voters_count)
    col2.metric("Total Candidates", candidates_count)

    consumer = create_kafka_consumer(topic_name)

    data = fetch_data_from_kafka(consumer)

    results = pd.DataFrame(data)

    # Remove rows with NaN values in key columns
    results_cleaned = results.dropna(subset=['candidate_id', 'total_votes'])

    # Verify if data exists after cleaning
    if results_cleaned.empty:
        st.warning("No valid data available for display.")
        return

    print("Cleaned Results")
    print(results_cleaned)

    # identify the leading candidate
    # results.loc[results.groupby('candidate_id')['total_votes'].idxmax()]
    # leading_candidate = results.loc[results['total_votes'].idxmax()]

    results_cleaned.loc[results_cleaned.groupby(
        'candidate_id')['total_votes'].idxmax()]

    leading_candidate = results_cleaned.loc[results_cleaned['total_votes'].idxmax(
    )]

    # display the leading candidate information
    st.markdown("""---""")
    st.header("Leading Candidate")
    col1, col2 = st.columns(2)
    with col1:
        st.image(leading_candidate['photo_url'], width=200)
    with col2:
        st.header(leading_candidate['candidate_name'])
        st.subheader(leading_candidate['party_affiliation'])
        st.subheader(f"Total Votes: {leading_candidate['total_votes']:.0f}")

    # display the voting statistics and visualization
    st.markdown("""---""")
    st.header("Voting Statistics")
    # results = results[['candidate_id', 'candidate_name',
    #                    'party_affiliation', 'total_votes']]
    # results = results.reset_index(drop=True)

    results_cleaned = results_cleaned[['candidate_id', 'candidate_name',
                                       'party_affiliation', 'total_votes']]
    results_cleaned = results_cleaned.reset_index(drop=True)

    # display charts - bar and donut
    col1, col2 = st.columns(2)

    with col1:
        # bar_fig = plot_bar_chart(results)
        bar_fig = plot_bar_chart(results_cleaned)
        st.pyplot(bar_fig)

    with col2:
        # donut_fig = plot_donut_chart(results)
        donut_fig = plot_donut_chart(results_cleaned)
        st.pyplot(donut_fig)

    # st.table(results)
    st.table(results_cleaned)

    # fetch data from Kafka on aggregated turnout by location
    location_consumer = create_kafka_consumer("aggregate_turnout_by_location")

    location_data = fetch_data_from_kafka(location_consumer)

    location_result = pd.DataFrame(location_data)

    # max location identification
    location_result = location_result.loc[location_result.groupby('state')[
        'count'].idxmax()]
    location_result = location_result.reset_index(drop=True)

    # display the location of voters
    st.header("Voter Locations")
    paginate_table(location_result)

    # update the session state
    st.session_state['last_update'] = time.time()


st.title("Realtime Election Voting Statistics")

topic = "voters_topic"
topic_name = "aggregated_votes_per_candidate"


side_bar()
update_data()
