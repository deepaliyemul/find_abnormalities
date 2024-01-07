import plotly.express as px
from datetime import datetime
import logging
import os 

class HTMLHelper(object):

    def browser_compatibility(self, text):
        htmlprepend = f"""
        <html>
        <head>
            <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-1BmE4kWBq78iYhFldvKuhfTAU6auU8tT94WrHftjDbrCEXSU1oBoqyl2QvZ6jIW3" cross
origin="anonymous">
            <meta charset="utf-8" />
        </head>
        <body>
            <div>
                <script type="text/javascript">window.PlotlyConfig = {{MathJaxConfig: \'local\'}};</script>
    
                <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            </div>
          {text}
        </body>
        </html>
        """
        return htmlprepend

    def add_page_headers(self, text, pheader):
        html = f"""<H1>{pheader}<br></H1><p style="font-size:11px">Last updated: {datetime.now().strftime("%Y-%m-%d %H:%M")}</p>"""
        return html + text

    def write_to_html(self, text, outfile):
        html_full = text
        if not outfile.endswith(".html"):
            outfile += ".html"
        # create directory is non existent
        if os.path.dirname(outfile):
            os.makedirs(os.path.dirname(outfile), exist_ok=True)
        with open(outfile, "w") as f:
            f.write(html_full)
            

    def dataframe_to_html(self, df, input_index=False, title=None, csv=False):
        html = """
            <style>
            table, th, td {
              border: 1px solid black;
              border-collapse: collapse;
              font-size: 14px;
              white-space:nowrap;
              text-align: left;
            }
            </style>
            """
        if title:
            html += "<header><h1>" + title + "</h1></header>"
        html += df.to_html(index=input_index)
        return html

    def figure_to_html(
        self,
        fig,
        title,
        height=800,
        width=1920,
        xaxis_title="Date and Time",
        yaxis_title="Value",
        ):
        fig.update_layout(
            xaxis_title=xaxis_title,
            yaxis_title=yaxis_title,
        )
        fig.layout.height = height
        fig.layout.width = width
        fig.layout.title = title
        html = f"""
                {fig.to_html(config=dict(displaylogo=False),include_plotlyjs=False, full_html=False)}
        """
        return html
