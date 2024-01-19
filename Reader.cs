using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml;

namespace XMLSQL
{
    internal class Reader
    {
        internal void StreamReader(string xmlPath, string xsdPath)
        {
            using (DataSet ds = new DataSet())
            using (FileStream xsdStream = File.OpenRead(xsdPath))
            using (XmlReader reader = XmlReader.Create(File.OpenRead(xmlPath)))
            {
                ds.ReadXmlSchema(xsdStream);
                ConcurrentBag<DataColumn> columns = ExtractColumns(ds);

                List<Row> aham = new List<Row>();

                ConcurrentBag<IdChain> idChains = new ConcurrentBag<IdChain>();

                IdChain root = IdChain.CreateRoot();
                idChains.Add(root);
                int callStack = 0;
                StreamReader(reader, ref columns, ref idChains, ref root, aham, ref callStack);
                Console.WriteLine(aham);
            }
        }

        private void StreamReader(XmlReader reader, ref ConcurrentBag<DataColumn> columns, ref ConcurrentBag<IdChain> idChains, ref IdChain lastIdChain, List<Row> rowContainer, ref int callStack)
        {
            callStack++;

            while (!reader.EOF)
            {
                SkipDontMatterNodes(reader, ref columns);

                if (reader.Depth > lastIdChain.Depth)
                {
                    StreamReaderDeeper(reader, ref columns, ref idChains, ref lastIdChain, rowContainer, ref callStack);
                    if (reader.Depth != lastIdChain.Depth)
                        break;
                    continue;
                }
                else
                    StreamReaderSameDepth(reader, ref columns, ref idChains, ref lastIdChain, rowContainer, ref callStack);
            }
        }

        private void StreamReaderDeeper(XmlReader reader, ref ConcurrentBag<DataColumn> columns, ref ConcurrentBag<IdChain> idChains, ref IdChain lastIdChain, List<Row> rowContainer, ref int callStack)
        {
            int idChainDepth = lastIdChain.Depth;
            string idChainOwnerName = lastIdChain.OwnerName;

            foreach (Row row in rowContainer.Where(r => r.Depth == idChainDepth && r.OwnerName.Equals(idChainOwnerName)).ToList())
            {
                PopulateRowSelfId(row, columns, lastIdChain);
                row.Complete();
            }

            PushCompletedRows(rowContainer);

            if (idChains.Count(idc => reader.Name.Equals(idc.OwnerName)) == 0)
            {
                lastIdChain = new IdChain(reader.Name, lastIdChain, reader.Depth);
                idChains.Add(lastIdChain);
            }
            else
            {
                lastIdChain = idChains.First(idc => idc.OwnerName.Equals(reader.Name));
            }


            //StreamReader(reader, ref columns, ref idChains, ref lastIdChain, rowContainer, ref callStack);
        }

        private void StreamReaderSameDepth(XmlReader reader, ref ConcurrentBag<DataColumn> columns, ref ConcurrentBag<IdChain> idChains, ref IdChain lastIdChain, List<Row> rowContainer, ref int callStack)
        {
            IdChain actualIdChain = lastIdChain;

            while (!reader.EOF)
            {
                SkipDontMatterNodes(reader, ref columns);

                if (idChains.Count(idc => reader.Name.Equals(idc.OwnerName)) == 0)
                {
                    actualIdChain = new IdChain(reader.Name, lastIdChain.Parent, reader.Depth);
                    idChains.Add(actualIdChain);
                }
                else
                    actualIdChain = idChains.First(idc => reader.Name.Equals(idc.OwnerName));

                idChains.Where(idc => reader.Name.Equals(idc.OwnerName)).ToList().ForEach(idc => idc.PlusId());


                rowContainer.Add(ExtractAttributes(reader, columns, actualIdChain));

                SkipDontMatterNodes(reader, ref columns);

                if (reader.Depth > actualIdChain.Depth)
                {
                    StreamReaderDeeper(reader, ref columns, ref idChains, ref actualIdChain, rowContainer, ref callStack);
                    //break;
                }
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

        private void SkipDontMatterNodes(XmlReader reader, ref ConcurrentBag<DataColumn> columns)
        {
            while (SkipNode(reader.NodeType) && !reader.EOF)
                reader.Read();

            while (columns.Count(column => column.Table.TableName.Equals(reader.Name)) == 0 && !reader.EOF)
                reader.Read();
        }

        private void PopulateRowSelfId(Row row, IEnumerable<DataColumn> columns, IdChain idChain)
        {
            row.SetAttribute(columns.First(column => column.ColumnName.Equals(String.Format("{0}_Id", idChain.OwnerName)) && column.Table.TableName.Equals(idChain.OwnerName)).Ordinal, idChain.Id);
        }

        private Row ExtractAttributes(XmlReader reader, IEnumerable<DataColumn> columns, IdChain idChain)
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

        private ConcurrentBag<DataColumn> ExtractColumns(DataSet dataSet)
        {
            using (dataSet)
            {
                DataTable[] tables = new DataTable[dataSet.Tables.Count];
                dataSet.Tables.CopyTo(tables, 0);

                return new ConcurrentBag<DataColumn>(tables.SelectMany(table =>
                {
                    using (table)
                    {
                        DataColumn[] columns = new DataColumn[table.Columns.Count];
                        table.Columns.CopyTo(columns, 0);
                        return columns;
                    }
                }));
            }
        }

        private Row PopRow(ConcurrentStack<Row> nonCompletedRows)
        {
            Row row;
            if (nonCompletedRows.TryPop(out row))
                return row;

            throw new InvalidDataException();
        }

        private void PushCompletedRows(List<Row> rows)
        {

            foreach (Row r in rows.Where(row => row.Completed).ToList())
            {
                PushRow(r);
                rows.Remove(r);
            }

            //rows.Where(row => row.Completed).ToList().ForEach(row => PushRow(row, ref columns));
            //rows.RemoveAll(row => row.Completed);
        }

        private bool PushRow(Row row)
        {
            if (row is null)
                return false;


            Console.WriteLine(row.OwnerName);
            Console.WriteLine(row.Depth);
            Console.WriteLine(row.Attributes);


            using (StreamWriter sw = new StreamWriter(@"C:\Users\jhonata.peres\Desktop\leitor XML\saida\saida.txt", true))
            {
                System.Text.StringBuilder sb = new StringBuilder();
                //for (int i = 0; i < row.Attributes.Length; i++)
                //{
                //    //sb.Append(String.Format("[{0}={1}]", i, row.Attributes[i]));
                //}

                for (int i = 0; i < row.Depth; i++)
                {
                    sb.Append("   ");
                }


                sw.WriteLine(String.Format("{0}{1} ID={3} Depth={2}  ", sb.ToString(), row.OwnerName, row.Depth, row.Id));
                sw.Close();
            }

            return true;
        }

        private class IdChainContainer
        {
            Dictionary<int, Dictionary<string, IdChain>> container;

            internal IdChainContainer()
            {
                this.container = new Dictionary<int, Dictionary<string, IdChain>>();
            }

            internal IdChain IdChain(int depth, string ownerName)
            {
                if (container.ContainsKey(depth))
                    if (container[depth].ContainsKey(ownerName))
                        return container[depth][ownerName];

                throw new ArgumentException();
            }

            internal void AddIdChain(int depth, string ownerName, IdChain idChain)
            {
                if (container.ContainsKey(depth))
                    if (container[depth].ContainsKey(ownerName))
                        throw new ConstraintException();
                    else
                        container[depth].Add(ownerName, idChain);
                else
                {
                    Dictionary<string, IdChain> aux = new Dictionary<string, IdChain>();
                    aux.Add(ownerName, idChain);
                    container.Add(depth, aux);
                }
            }

        }

        internal class Row
        {
            private bool completed;

            internal bool Completed
            {
                get { return completed; }
            }

            private readonly int id;

            internal int Id
            {
                get { return id; }
            }

            private readonly int depth;

            internal int Depth
            {
                get { return depth; }
            }

            private readonly string ownerName;

            internal string OwnerName
            {
                get { return ownerName; }
            }

            private readonly object[] attributes;

            internal object[] Attributes
            {
                get { return attributes; }
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

            private readonly IdChain parent;

            internal IdChain Parent
            {
                get
                {
                    if (root) return this;
                    return parent;
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

    }

}
