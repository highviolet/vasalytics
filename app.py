import streamlit as st
import pandas as pd
import plost
import altair as alt

st.set_page_config(page_title="Vasalytics", layout="wide")
st.title("Vasaloppet 2022")

START_GROUPS = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]


SPLITS = [
    "Mål",
    "Högsta punkten",
    "Smågan",
    "Mångsbodarna",
    "Risberg",
    "Evertsberg",
    "Oxberg",
    "Hökberg",
    "Eldris",
]
# SPLIT_COLUMNS = ['split_Högsta punkten', 'split_Smågan', 'split_Mångsbodarna', 'split_Risberg', 'split_Evertsberg', 'split_Oxberg', 'split_Hökberg', 'split_Eldris', 'split_Mora Förvarning']


@st.cache_data
def get_frame() -> pd.DataFrame:
    df = pd.read_pickle("clean.pkl")
    return df


def start_group_bar_chart(df: pd.DataFrame):
    df_bar = df.groupby("start_group", as_index=False).count()[["start_group", "name"]]
    df_bar.rename(columns={"name": "count"}, inplace=True)
    chart = (
        alt.Chart(df_bar)
        .mark_bar()
        .encode(x=alt.X("start_group:O", title=None), y=alt.Y("count:Q", title=None))
        .properties(title="Anmälda per led")
    )
    st.altair_chart(chart, use_container_width=True)


def time_histogram(df: pd.DataFrame):
    start_group = st.selectbox(label="Start group", options=START_GROUPS)

    df = df[df.start_group == start_group]

    chart = (
        alt.Chart(df)
        .mark_bar(opacity=0.3, binSpacing=0)
        .encode(
            alt.X("time:Q", bin=alt.Bin(anchor=3, step=0.5)),
            alt.Y("count()", stack=None),
            alt.Color("start_group:O"),
        )
        .add_selection(alt.selection_single())
    )
    st.altair_chart(chart, use_container_width=True)


def time_histo_layered_by_group(df):
    chart = (
        alt.Chart(df)
        .mark_bar(opacity=0.3, binSpacing=0)
        .encode(
            alt.X("time:Q", bin=alt.Bin(step=0.25), title=None),
            alt.Y("count()", stack=None, title=None),
            alt.Color("start_group:N"),
        )
        .properties(title="Fördelning sluttid per startgrupp")
        .interactive()
    )
    st.altair_chart(chart, use_container_width=True)


def time_by_group_boxplot(df):
    chart = (
        alt.Chart(df)
        .mark_boxplot(extent="min-max")
        .encode(x="start_group:O", y="time:Q")
    )
    st.altair_chart(chart, use_container_width=True)


def status_donut(df):
    df = df.groupby("race_status", as_index=False)["name"].count()
    print(df)
    chart = (
        alt.Chart(df)
        .mark_arc(innerRadius=50)
        .encode(
            theta=alt.Theta(field="name", type="quantitative"),
            color=alt.Color(field="race_status", type="nominal", legend=None),
        )
        .properties(title="Anmälda per status")
        .interactive()
    )
    st.altair_chart(chart, use_container_width=True)


@st.cache_data
def time_by_group_ridgeline(df, col="time"):
    step = 40
    overlap = 1

    print(df.columns)

    df = df[~df[col].isna()]

    chart = (
        alt.Chart(df, height=step)
        .transform_joinaggregate(mean_time=f"mean({col})", groupby=["start_group"])
        .transform_bin(["bin_max", "bin_min"], bin=alt.Bin(step=0.25), field=f"{col}")
        .transform_aggregate(
            value="count()",
            groupby=["start_group", "mean_time", "bin_min", "bin_max"],
        )
        .transform_impute(
            impute="value",
            groupby=["start_group", "mean_time"],
            key="bin_min",
            value=0,
        )
        .mark_area(
            interpolate="monotone", fillOpacity=0.8, stroke="lightgray", strokeWidth=0.5
        )
        .encode(
            alt.X("bin_min:Q", bin="binned", title="Time"),
            alt.Y("value:Q", scale=alt.Scale(range=[step, -step * overlap]), axis=None),
            alt.Fill(
                "mean_time:Q",
                legend=None,
                scale=alt.Scale(domain=[30, 5], scheme="redyellowblue"),
            ),
        )
        .facet(
            row=alt.Row(
                "start_group:O",
                title=None,
                # header=alt.Header(labelAngle=0),  # , labelAlign="right"),
            )
        )
        .properties(title="Sluttid per startgrupp", bounds="flush")
        .configure_facet(spacing=0)
        .configure_view(stroke=None)
    )

    st.altair_chart(chart, use_container_width=True)


def generate_page():
    df = get_frame()

    [col1, col2, col3] = st.columns(3)

    with col1:
        start_group_bar_chart(df)
    with col2:
        status_donut(df)
    with col3:
        time_histo_layered_by_group(df)

    [col1, col2] = st.columns(2)
    with col1:
        split1 = st.selectbox(label="Split", options=SPLITS, index=0, key="split1")

        if split1 == "Mål":
            col = "time"
        else:
            col = "split_" + split1

        time_by_group_ridgeline(df, col)

    with col2:
        split2 = st.selectbox(label="Split", options=SPLITS, index=1, key="split2")

        if split2 == "Mål":
            col = "time"
        else:
            col = "split_" + split2

        time_by_group_ridgeline(df, col)

    # time_by_group_boxplot(df)
    # time_by_group_violin(df)
    # time_histogram(df)


if __name__ == "__main__":
    generate_page()
