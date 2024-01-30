using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Xml;
using static f2.util.ObserverUtils;

namespace f2.service
{
    internal class XmlReader : IObservable<XmlReader.Row>
    {
        private List<IObserver<XmlReader.Row>> rowObservers = new List<IObserver<XmlReader.Row>>();

        private ProcessObservable processObservable = new ProcessObservable();

        internal XmlReader SetProcessObervable(ProcessObservable processObservable)
        {
            this.processObservable = processObservable;
            return this;
        }

        internal void StreamReader(string xmlPath, string xsdPath)
        {
            DiscoveryLinesCount(xmlPath);

            using (DataSet ds = new DataSet())
            using (FileStream xsdStream = File.OpenRead(xsdPath))
            using (FileStream xmlStream = File.OpenRead(xmlPath))
            using (System.Xml.XmlReader reader = System.Xml.XmlReader.Create(xmlStream))
            {
                ds.ReadXmlSchema(xsdStream);
                Dictionary<string, List<DataColumn>> columns = ExtractColumns(ds);

                List<Row> rowContainer = new List<Row>();

                Dictionary<string, IdChain> idChains = new Dictionary<string, IdChain>();
                IdChain root = IdChain.CreateRoot();
                idChains.Add("", root);
                StreamReader(reader, ref columns, ref idChains, ref root, rowContainer);
                rowObservers.ForEach(observer => observer.OnCompleted());
            }
        }

        private void DiscoveryLinesCount(string xmlPath)
        {
            using (FileStream xmlStream = File.OpenRead(xmlPath))
            using (System.Xml.XmlReader reader = System.Xml.XmlReader.Create(xmlStream))
            {
                while (!reader.EOF)
                    reader.Read();

                IXmlLineInfo lineInfo = (IXmlLineInfo)reader;
                processObservable.NewProcess("Leitura/Conversão XML", lineInfo.LineNumber);

                reader.Close();
            }
        }

        private void StreamReader(System.Xml.XmlReader reader, ref Dictionary<string, List<DataColumn>> columns, ref Dictionary<string, IdChain> idChains, ref IdChain lastIdChain, List<Row> rowContainer)
        {
            IXmlLineInfo lineInfo = (IXmlLineInfo)reader;

            while (!reader.EOF)
            {
                SkipDontMatterNodes(reader, ref columns);

                processObservable.SetStep(lineInfo.LineNumber, 1);

                if (reader.Depth > lastIdChain.Depth)
                    StreamReaderDeeper(reader, ref columns, ref idChains, ref lastIdChain, rowContainer);
                else
                    StreamReaderSameDepth(reader, ref columns, ref idChains, ref lastIdChain, rowContainer);
            }

            processObservable.SetStep(lineInfo.LineNumber, 1);

            reader.Close();
        }

        private void StreamReaderDeeper(System.Xml.XmlReader reader, ref Dictionary<string, List<DataColumn>> columns, ref Dictionary<string, IdChain> idChains, ref IdChain lastIdChain, List<Row> rowContainer)
        {
            int idChainDepth = lastIdChain.Depth;
            string idChainOwnerName = lastIdChain.OwnerName;

            foreach (Row row in rowContainer.Where(r => r.Depth == idChainDepth && r.OwnerName.Equals(idChainOwnerName)).ToList())
            {
                PopulateRowSelfId(row, columns[idChainOwnerName], lastIdChain);
                row.Complete();
            }

            PushCompletedRows(rowContainer);

            if (!idChains.ContainsKey(reader.Name))
                idChains.Add(reader.Name, new IdChain(reader.Name, lastIdChain, reader.Depth));
            else
                idChains[reader.Name].Parent = lastIdChain;

            lastIdChain = idChains[reader.Name];
        }

        private void StreamReaderSameDepth(System.Xml.XmlReader reader, ref Dictionary<string, List<DataColumn>> columns, ref Dictionary<string, IdChain> idChains, ref IdChain lastIdChain, List<Row> rowContainer)
        {
            IdChain actualIdChain = lastIdChain;

            while (!reader.EOF)
            {
                SkipDontMatterNodes(reader, ref columns);

                if (!idChains.ContainsKey(reader.Name))
                    idChains.Add(reader.Name, new IdChain(reader.Name, actualIdChain.Parent, reader.Depth));

                idChains[reader.Name].PlusId();

                actualIdChain = idChains[reader.Name];

                rowContainer.Add(ExtractAttributes(reader, columns[reader.Name], actualIdChain));

                SkipDontMatterNodes(reader, ref columns);

                if (reader.Depth > actualIdChain.Depth)
                    StreamReaderDeeper(reader, ref columns, ref idChains, ref actualIdChain, rowContainer);
                else
                {
                    rowContainer
                        .Where(row => reader.Depth <= row.Depth)
                        .ToList()
                        .ForEach(row => row.Complete());

                    PushCompletedRows(rowContainer);
                }

                if (reader.Depth < actualIdChain.Depth)
                {
                    while (actualIdChain.Depth > reader.Depth)
                        actualIdChain = actualIdChain.Parent;
                    lastIdChain = actualIdChain;
                    break;
                }
            }
        }

        private void SkipDontMatterNodes(System.Xml.XmlReader reader, ref Dictionary<string, List<DataColumn>> columns)
        {
            while (SkipNode(reader.NodeType) && !reader.EOF)
                reader.Read();

            while (!columns.ContainsKey(reader.Name) && !reader.EOF)
                reader.Read();
        }

        private void PopulateRowSelfId(Row row, List<DataColumn> columns, IdChain idChain)
        {
            row.SetAttribute(columns.First(column => column.ColumnName.Equals(String.Format("{0}_Id", idChain.OwnerName)) && column.Table.TableName.Equals(idChain.OwnerName)).Ordinal, idChain.Id);
        }

        private Row ExtractAttributes(System.Xml.XmlReader reader, List<DataColumn> columns, IdChain idChain)
        {
            object[] attributes = new object[columns.Where(column => column.Table.TableName.Equals(idChain.OwnerName)).Count()];

            if (reader.Depth > idChain.Parent.Depth && !idChain.Parent.Root)
                attributes[columns.First(column => column.ColumnName.Equals(String.Format("{0}_Id", idChain.Parent.OwnerName)) && column.Table.TableName.Equals(idChain.OwnerName)).Ordinal] = idChain.Parent.Id;

            while (reader.MoveToNextAttribute())
                attributes[columns.First(column => column.ColumnName.Equals(reader.Name) && column.Table.TableName.Equals(idChain.OwnerName)).Ordinal] = reader.HasValue ? reader.Value : null;

            reader.Read();
            return new Row(idChain.Id, idChain.Depth, idChain.OwnerName, attributes);
        }

        private bool SkipNode(XmlNodeType nodeType)
        {
            return XmlNodeType.None.Equals(nodeType) ||
                XmlNodeType.Whitespace.Equals(nodeType) ||
                XmlNodeType.EndElement.Equals(nodeType);
        }

        private Dictionary<string, List<DataColumn>> ExtractColumns(DataSet dataSet)
        {
            using (dataSet)
            {
                DataTable[] tables = new DataTable[dataSet.Tables.Count];
                dataSet.Tables.CopyTo(tables, 0);

                return tables.GroupBy(table => table.TableName).ToDictionary(group => group.Key,
                    group => group.ToList()
                    .SelectMany(table =>
                    {
                        DataColumn[] columns = new DataColumn[table.Columns.Count];
                        table.Columns.CopyTo(columns, 0);
                        return columns.ToList();
                    })
                    .ToList());
            }
        }

        private void PushCompletedRows(List<Row> rows)
        {
            rows.Where(row => row.Completed).ToList().ForEach(row => PushRow(row));
            rows.RemoveAll(row => row.Completed);
        }

        private void PushRow(Row row)
        {
            if (row is null)
                return;

            rowObservers.ForEach(observer => observer.OnNext(row));
        }

        public IDisposable Subscribe(IObserver<Row> observer)
        {
            if (!rowObservers.Contains(observer))
                rowObservers.Add(observer);
            return new Unsubscriber<Row>(rowObservers, observer);
        }

        private class IdChain
        {
            private int id;

            internal int Id
            {
                get { return id; }
            }

            private readonly string ownerName;

            internal string OwnerName
            {
                get { return ownerName; }
            }

            private bool hasChildren;

            internal bool HasChildren
            {
                get { return hasChildren; }
            }

            private readonly bool root;

            internal bool Root
            {
                get { return root; }
            }

            private readonly int depth;

            internal int Depth
            {
                get { return depth; }
            }

            private IdChain parent;

            internal IdChain Parent
            {
                get
                {
                    if (root) return this;
                    return parent;
                }
                set
                {
                    this.parent = value;
                }
            }

            internal IdChain(string ownerName, IdChain parent, int depth)
            {
                parent.hasChildren = true;
                this.id = 0;
                this.ownerName = ownerName;
                this.parent = parent;
                this.depth = depth;
            }

            private IdChain()
            {
                this.root = true;
                this.ownerName = "root";
                this.depth = -1;
            }

            internal void PlusId()
            {
                this.id++;
            }

            internal static IdChain CreateRoot()
            {
                return new IdChain();
            }

        }

        internal class Row
        {
            private readonly int id;

            public int Id
            {
                get { return id; }
            }

            private readonly string ownerName;

            public string OwnerName
            {
                get { return ownerName; }
            }

            private readonly object[] attributes;

            public object[] Attributes
            {
                get { return attributes; }
            }

            private bool completed;

            internal bool Completed
            {
                get { return completed; }
            }

            private readonly int depth;

            internal int Depth
            {
                get { return depth; }
            }

            internal Row(int id, int depth, string ownerName, object[] attributes)
            {
                this.id = id;
                this.depth = depth;
                this.ownerName = ownerName;
                this.attributes = attributes;
            }

            internal void SetAttribute(int index, object value)
            {
                this.attributes[index] = value;
            }

            internal void Complete()
            {
                this.completed = true;
            }

        }

    }


}
