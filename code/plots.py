# code/plots.py

import plotly.express as px
import plotly.graph_objects as go
from utils import format_bytes

def create_top_tables_bar_chart(df, ranking_option):
    """Creates a stacked bar chart of the top 20 tables by storage size."""
    df_copy = df.copy()
    df_copy['total_storage_size'] = df_copy['data_folder_size'] + df_copy['metadata_folder_size']

    if ranking_option == "Data + Metadata":
        ranking_col = 'total_storage_size'
    elif ranking_option == "Data Only":
        ranking_col = 'data_folder_size'
    else:  # Metadata Only
        ranking_col = 'metadata_folder_size'

    top_20_df = df_copy.nlargest(20, ranking_col)
    plot_df = top_20_df[['full_table_name', 'data_folder_size', 'metadata_folder_size']].melt(
        id_vars='full_table_name', var_name='Component', value_name='Size'
    )
    plot_df['Component'] = plot_df['Component'].map({
        'data_folder_size': 'Data', 'metadata_folder_size': 'Metadata'
    })
    plot_df['Size (GB)'] = plot_df['Size'] / (1024**3)

    fig = px.bar(
        plot_df,
        y='full_table_name', x='Size (GB)', color='Component',
        labels={'full_table_name': 'Table Name', 'Size (GB)': 'Total Storage Size (GB)'},
        color_discrete_map={'Data': '#5555F9', 'Metadata': '#FF550D'}
    )
    fig.update_layout(
        yaxis={'categoryorder': 'total ascending'}, legend_title_text='Component',
        height=700, margin=dict(t=50),
        legend=dict(orientation="v", yanchor="bottom", y=0.01, xanchor="right", x=0.99)
    )
    fig.update_traces(hovertemplate='<b>%{y}</b><br>%{data.name}: %{x:.2f} GB<extra></extra>')
    
    # Add total size annotations
    totals = top_20_df.set_index('full_table_name')['total_storage_size'] / (1024**3)
    for table_name in totals.index:
        total_size = totals[table_name]
        fig.add_annotation(
            x=total_size, y=table_name, text=f"{total_size:.0f} GB", showarrow=False,
            xshift=5, xanchor='left', font=dict(color="white", size=11)
        )
    return fig


def create_db_composition_treemap(df):
    """Creates a nested treemap showing data and metadata composition by database."""
    agg_df = df.groupby('db_name')[['data_folder_size', 'metadata_folder_size']].sum().reset_index()
    plot_df = agg_df.melt(id_vars='db_name', var_name='Component', value_name='Size')
    plot_df['Component'] = plot_df['Component'].map({
        'data_folder_size': 'Data', 'metadata_folder_size': 'Metadata'
    })

    # ✨ NEW: Create a dedicated column for the hover text
    plot_df['Formatted Size'] = plot_df['Size'].apply(format_bytes)

    fig = px.treemap(
        plot_df,
        path=[px.Constant("All Databases"), 'db_name', 'Component'],
        values='Size',
        color='Component',
        color_discrete_map={'Data': '#5555F9', 'Metadata': '#FF550D', '(?)': '#FFFFFF'},
        # ✨ MODIFIED: Pass the new column to custom_data here
        custom_data=['Formatted Size']
    )

    # The hovertemplate now works correctly because it can find the customdata
    fig.update_traces(
        hovertemplate="<b>%{label}</b><br>Size: %{customdata[0]}<extra></extra>",
        textfont_color='black',
        tiling_pad=0
    )
    
    fig.update_layout(height=700, margin=dict(t=50))
    return fig


def create_small_files_scatter_plot(df, threshold):
    """Creates an impact scatter plot to identify high-priority tables for compaction."""
    df_plot = df[df['data_folder_count'] > 0].copy()

    if df_plot.empty:
        fig = go.Figure()
        fig.add_annotation(text="No data with files to analyze.", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        return fig

    fig = px.scatter(
        df_plot,
        x='avg_file_size_mb',
        y='data_folder_count',
        size='data_folder_size',
        color='db_name',
        hover_name='full_table_name',
        log_y=True, # Logarithmic scale is key to handling wide range of file counts
        title="Compaction Impact Analysis",
        subtitle="*Tables in the top-left quadrant are the highest priority for compaction.",
        height=600,
        labels={
            'avg_file_size_mb': 'Average File Size (MB)',
            'data_folder_count': 'Number of Data Files (Log Scale)',
            'data_folder_size': 'Total Data Size'
        },
        custom_data=['avg_file_size_mb', 'data_folder_count','data_folder_size_readable']
    )
    
    fig.update_traces(
        hovertemplate="<br>".join([
            "<b>%{hovertext}</b>",
            "Average File Size (MB): %{x:.3f} MB",
            "Number of Data Files: %{y:.0f}",
            "Total Data Size: %{customdata[2]}",  # <-- This displays your readable string
            "<extra></extra>"
        ])
    )

    # Add a vertical line for the small file threshold
    fig.add_vline(x=threshold, line_width=2, line_dash="dash", line_color="red",
                  annotation_text=f"Threshold: {threshold}MB", annotation_position="top left")

    fig.update_layout(legend_title_text='Database')
    return fig

def create_hotspot_treemap(df):
    """Creates a treemap to identify small file hotspots."""
    df_plot = df[df['data_folder_count'] > 0].copy()

    if df_plot.empty:
        fig = go.Figure()
        fig.add_annotation(text="No data with files to analyze.", xref="paper", yref="paper", x=0.5, y=0.5, showarrow=False)
        return fig

    # Set a reasonable upper limit for the color scale, e.g., 128MB
    # This prevents one large table from skewing the color of all others.
    color_range_max = 128 
    df_plot['color_avg_size'] = df_plot['avg_file_size_mb'].clip(upper=color_range_max)

    fig = px.treemap(
        df_plot,
        path=[px.Constant("All Tables"), 'db_name', 'full_table_name'],
        values='data_folder_count',
        color='color_avg_size',
        color_continuous_scale='RdYlGn', # Red->Yellow->Green scale; smaller is redder
        color_continuous_midpoint=color_range_max / 2,
        hover_name='full_table_name',
        custom_data=['avg_file_size_mb', 'data_folder_count','data_folder_size_readable'],
        title="Small File Hotspot Analysis",
        subtitle="*Large, red rectangles represent tables with many small files.",
        height=600,
    )

    fig.update_traces(
        hovertemplate="<br>".join([
            "<b>%{hovertext}</b>",
            "Average File Size (MB): %{customdata[0]:.3f} MB",
            "Number of Data Files: %{customdata[1]}",
            "Total Data Size: %{customdata[2]}",  # <-- This displays your readable string
            "<extra></extra>"
        ])
    )

    fig.update_layout(
#        title="Small File Hotspot Analysis",
        coloraxis_colorbar=dict(
            title="Avg. File Size (MB)"
        )
    )
    return fig

def create_snapshot_scatter_plot(df, x_axis, y_axis):
    """Creates a dynamic scatter plot based on user-selected axes."""
    
    def format_label(column_name):
        return column_name.replace('_', ' ').title()

    fig = px.scatter(
        df, 
        x=x_axis, 
        y=y_axis,
        color='db_name', 
        size='data_folder_size', 
        hover_name='full_table_name',
        labels={
            x_axis: format_label(x_axis),
            y_axis: format_label(y_axis),
            "db_name": "Database", 
            "data_folder_size": "Data Size"
        },
        title=f"{format_label(y_axis)} vs. {format_label(x_axis)}",
      height=600,
    )
    fig.update_layout(legend_title_text='Database')
    
    fig.update_traces(
        hovertemplate="<b>%{hovertext}</b><br><br>" +
                      "Database: %{customdata[0]}<br>" +
                      f"{format_label(x_axis)}: %{{x}}<br>" +
                      f"{format_label(y_axis)}: %{{y}}<extra></extra>",
        customdata=df[['db_name']]
    )
    return fig
