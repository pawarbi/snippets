import altair as alt
import pandas as pd


data = {
  'Configuration': ['Spark Default', 'Spark Partitioned', 'Spark NEE', 'Spark No V-ORDER', 'Spark No V-ORDER & OptimizeWrite',
                   'Polars 4 Cores Default', 'Polars 8 Cores Default', 'Polars 8M Groups', 'Polars 16M Groups',
                   'Daft', 'Daft+Polars'],
  'Write Time': [242, 180, 135, 153, 127, 80, 80, 80, 80, 80, 81],
  'Read Time': [6, 6, 6, 10, 10, 12, 12, 7, 7, 10, 12],
  'Engine': ['Spark', 'Spark', 'Spark', 'Spark','Spark', 
           'Polars', 'Polars', 'Polars', 'Polars',
           'Daft', 'Daft+Polars']
}

df = pd.DataFrame(data).sort_values('Write Time', ascending=True)


color_scale = alt.Scale(
  domain=['Spark', 'Polars', 'Daft', 'Daft+Polars'],
  range=['#F28E2B', '#4E79A7', '#59A14F', '#B07AA1']
)

bar = alt.Chart(df).mark_bar().encode(
  x=alt.X('Configuration:N', sort='-y', 
          axis=alt.Axis(labelAngle=-45, labelFontSize=11)),
  y=alt.Y('Write Time:Q', 
          title='Write Time (seconds) [Lower is better]',
          axis=alt.Axis(labelFontSize=11)),
  color=alt.Color('Engine:N', 
                 scale=color_scale, 
                 legend=alt.Legend(title="Engine", labelFontSize=11, titleFontSize=12)),
  tooltip=[
      alt.Tooltip('Configuration:N', title='Config'),
      alt.Tooltip('Write Time:Q', title='Write Time (s)'),
      alt.Tooltip('Read Time:Q', title='Read Time (s)'),
      alt.Tooltip('Engine:N', title='Engine')
  ]
).properties(
  width=500,
  title=alt.TitleParams('Write Performance by Configuration',
                       fontSize=14)
)

bar_text = bar.mark_text(
  align='center',
  baseline='bottom',
  dy=-5,
  fontSize=11
).encode(
  text=alt.Text('Write Time:Q', format='.0f')
)

nearest = alt.selection_point(nearest=True, on='mouseover',
                           fields=['Write Time', 'Read Time', 'Engine'])

scatter_base = alt.Chart(df).transform_calculate(
    jitter_x='datum["Write Time"] + (random() - 0.5) * 3', #jitter
    jitter_y='datum["Read Time"] + (random() - 0.5) * 0.5'
).encode(
    x=alt.X('jitter_x:Q',
            title='Write Time (seconds) [Lower is better]',
            scale=alt.Scale(domain=[0, 260]),
            axis=alt.Axis(labelFontSize=11)),
    y=alt.Y('jitter_y:Q',
            title='Avg Read Time (seconds) [Lower is better]',
            scale=alt.Scale(domain=[0, 15]),
            axis=alt.Axis(labelFontSize=11)),
    color=alt.Color('Engine:N', scale=color_scale)
)


scatter_points = scatter_base.mark_point(size=150, filled=True).encode(
    shape=alt.Shape('Engine:N', scale=alt.Scale(
        domain=['Spark', 'Polars', 'Daft', 'Daft+Polars'],
        range=['square', 'circle', 'triangle-up', 'diamond']
    )),
    opacity=alt.condition(nearest, alt.value(1), alt.value(0.5)),
    tooltip=[
        alt.Tooltip('Configuration:N', title='Config'),
        alt.Tooltip('Write Time:Q', title='Write Time (s)', format='.1f'),
        alt.Tooltip('Read Time:Q', title='Read Time (s)', format='.1f'),
        alt.Tooltip('Engine:N', title='Engine')
    ]
).add_params(nearest)

# Polars cluster (Write Time ~80)
text_polars_1 = scatter_base.transform_filter(
    'datum.Engine === "Polars" && datum["Read Time"] >= 11'
).mark_text(
    align='left',
    baseline='middle',
    dx=7,
    dy=-20,
    fontSize=11
).encode(text='Configuration:N')

text_polars_2 = scatter_base.transform_filter(
    'datum.Engine === "Polars" && datum["Read Time"] < 9'
).mark_text(
    align='left',
    baseline='middle',
    dx=7,
    dy=20,
    fontSize=11
).encode(text='Configuration:N')

# Spark cluster
text_spark_1 = scatter_base.transform_filter(
    'datum.Engine === "Spark" && datum["Write Time"] > 150'
).mark_text(
    align='left',
    baseline='middle',
    dx=7,
    dy=-10,
    fontSize=11
).encode(text='Configuration:N')

text_spark_2 = scatter_base.transform_filter(
    'datum.Engine === "Spark" && datum["Write Time"] <= 150 && datum["Write Time"] > 130'
).mark_text(
    align='left',
    baseline='middle',
    dx=7,
    dy=0,
    fontSize=11
).encode(text='Configuration:N')

text_spark_3 = scatter_base.transform_filter(
    'datum.Engine === "Spark" && datum["Write Time"] <= 130'
).mark_text(
    align='left',
    baseline='middle',
    dx=7,
    dy=10,
    fontSize=11
).encode(text='Configuration:N')

# Daft points
text_daft = scatter_base.transform_filter(
    'datum.Engine === "Daft" || datum.Engine === "Daft+Polars"'
).mark_text(
    align='left',
    baseline='middle',
    dx=7,
    dy=0,
    fontSize=11
).encode(text='Configuration:N')


scatter = (scatter_points + text_polars_1 + text_polars_2 + text_spark_1 + 
          text_spark_2 + text_spark_3 + text_daft).properties(
    width=500,
    title=alt.TitleParams('Delta Write Time vs Avg DL Read Time Performance',
                         fontSize=14)
)

# Final
final = alt.vconcat(
    (bar + bar_text),
    scatter
).properties(
    title=alt.TitleParams(
        'Fabric Delta Write & Direct Lake Read Performance(Read 600M rows → Write 50M rows)',
        subtitle=["", 'Write Delta lake to a lakehouse and read with a DAX query in a custom Direct Lake model',
                "---------------------------------------------------------------------------------------------------------------------------------" , 
                'All are cold cache', 'Python 2 vCores ran out of memory','Lower values are better for both metrics'],
        fontSize=18
    )
).configure_view(
    strokeWidth=0
).configure_axis(
    titleFontSize=12,
    labelFontSize=11
).configure_legend(
    titleFontSize=12,
    labelFontSize=11
).configure(
    background='#f9f9f9'
)

final
