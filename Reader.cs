using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
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
                StreamReader(reader, ref columns, IdChain.CreateRoot(), new ConcurrentStack<Row>()); //
            }
        }

        private void StreamReader(XmlReader reader, ref ConcurrentBag<DataColumn> columns, IdChain idChain, ConcurrentStack<Row> rowContainer) //
        {

            while (!reader.EOF)
            {
                SkipDontMatterNodes(reader, ref columns);

                if (reader.Depth == idChain.Depth && idChain.OwnerName.Equals(reader.Name))
                    idChain.PlusId();

                if (reader.Depth > idChain.Depth)
                {
                    Row row;

                    if (rowContainer.TryPop(out row))
                    {
                        PopulateRowSelfId(row, columns, idChain);
                        PushRow(row);
                    }

                    StreamReader(reader, ref columns, new IdChain(reader.Name, idChain, reader.Depth), rowContainer);
                    if (reader.Depth != idChain.Depth)
                        break;
                    continue;
                }
                else
                {
                    StreamReaderSameDepth(reader, ref columns, idChain.Parent, rowContainer);
                }
            }
        }

        private void StreamReaderSameDepth(XmlReader reader, ref ConcurrentBag<DataColumn> columns, IdChain parentIdChain, ConcurrentStack<Row> rowContainer)
        {
            HashSet<IdChain> sameDepthIdChains = new HashSet<IdChain>();

            while (!reader.EOF)
            {
                SkipDontMatterNodes(reader, ref columns);

                IdChain actualIdChain;
                if (sameDepthIdChains.Count(idc => idc.Depth == reader.Depth && reader.Name.Equals(idc.OwnerName)) == 0)
                {
                    actualIdChain = new IdChain(reader.Name, parentIdChain, reader.Depth);
                    sameDepthIdChains.Add(actualIdChain);
                }
                else
                    actualIdChain = sameDepthIdChains.First(idc => idc.Depth == reader.Depth && reader.Name.Equals(idc.OwnerName));

                if (reader.Depth == actualIdChain.Depth && actualIdChain.OwnerName.Equals(reader.Name))
                    actualIdChain.PlusId();

                rowContainer.Push(ExtractAttributes(reader, columns, actualIdChain));

                if (reader.Depth != actualIdChain.Depth)
                {
                    Row row;
                    while (rowContainer.TryPop(out row))
                        PushRow(row);
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
            //return new Row(iDChain.Id(ownerName), iDChain.Depth, ownerName, attributes);
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

        private bool PushRow(Row row)
        {
            if (row is null)
                return false;


            Console.WriteLine(row.OwnerName);
            Console.WriteLine(row.Attributes);
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
                //this.containerIds = new Dictionary<string, int>();
                //this.containerIds.Add(ownerName, 0);
            }

            private IdChain()
            {
                this.root = true;
                this.ownerName = "root";
                this.depth = -1;
                //this.containerIds = new Dictionary<string, int>();
                //this.containerIds.Add("root", -1);
            }

            internal void PlusId()
            {
                this.id++;
                //if (this.containerIds.ContainsKey(ownerName))
                //    this.containerIds[ownerName]++;
                //else
                //    this.containerIds.Add(ownerName, 1);
            }

            //internal int Id(string ownerName)
            //{
            //    if (this.containerIds.ContainsKey(ownerName))
            //        return this.containerIds[ownerName];

            //    throw new ArgumentException();
            //}

            internal static IdChain CreateRoot()
            {
                return new IdChain();
            }

        }



    }

}
