import requests
import pandas as pd
import holoviews as hv
from holoviews import opts, dim
from bokeh.plotting import show

hv.extension('bokeh')
hv.output(size=200)

df = pd.read_csv(r"C:\Users\Jeff\Desktop\新建 Microsoft Excel 工作表.csv", index_col=0, header=0)
print(df)
node = pd.DataFrame(df['from'].append(df['to']), columns=['节点']).drop_duplicates().reset_index(drop=True)

print(node)

for i in range(0,6):
    res = node.loc(i, '节点')
    print(res)
    # if node.loc(i, '节点')=='渝中区':
    #     print(111111)

links = df
nodes = hv.Dataset(node, '节点')
data = hv.Dataset(df,  ['from','to'])

chord = hv.Chord((data, nodes)).select(value=(5, None))
chord.opts(
    opts.Chord(
        labels='节点',
        cmap='Category20',
        edge_cmap='Category20',
        edge_line_width=dim('value'), #'edge_line_width', 'frame_width', 'width'
        edge_color=dim('from').str(),
        node_color=dim('节点').str()
    )
)
# show(hv.render(chord))