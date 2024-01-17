using System.Collections.Concurrent;
using System.Data;
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
                StreamReader(reader, ref columns, IDChain.CreateRoot(), new ConcurrentStack<Row>()); //
            }
        }

        private void StreamReader(XmlReader reader, ref ConcurrentBag<DataColumn> columns, IDChain iDChain, ConcurrentStack<Row> nonCompletedRows) //
        {

            while (!reader.EOF)
            {
                while (SkipNode(reader.NodeType) && !reader.EOF)
                    reader.Read();

                while (columns.Count(column => column.Table.TableName.Equals(reader.Name)) == 0 && !reader.EOF)
                    reader.Read();

                if (reader.Depth == iDChain.Depth && reader.Name.Equals(iDChain.OwnerName))
                    iDChain.PlusId();

                if (reader.Depth > iDChain.Depth)
                    StreamReader(reader, ref columns, new IDChain(iDChain, reader.Name, reader.Depth), nonCompletedRows);
                else
                    nonCompletedRows.Push(ExtractAttributes(reader, columns.Where(column => column.Table.TableName.Equals(reader.Name)).ToArray(), iDChain));

                reader.Read();
            }


            //StreamReader(reader, ref columns, new IDChain(iDChain.Parent, reader.Name, reader.Depth)); //, nonCompletedRows

            //nonCompletedRows.Push(ExtractAttributes(reader, columns.Where(column => column.Table.TableName.Equals(reader.Name)).ToArray(), iDChain));


            //if (XmlNodeType.EndElement.Equals(reader.NodeType))
            //{
            //    PopRow(nonCompletedRows, reader.Name);

            //    break;
            //}
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

        private Row ExtractAttributes(XmlReader reader, IEnumerable<DataColumn> columns, IDChain iDChain)
        {
            object[] attributes = new object[columns.Count()];

            if (iDChain.HasChildren)
                attributes[columns.First(column => column.ColumnName.Equals(String.Format("{0}_Id", iDChain.OwnerName)) && column.Table.TableName.Equals(iDChain.OwnerName)).Ordinal] = iDChain.Id;

            if (reader.Depth > iDChain.Parent.Depth && !iDChain.Parent.Root)
                attributes[columns.First(column => column.ColumnName.Equals(String.Format("{0}_Id", iDChain.Parent.OwnerName)) && column.Table.TableName.Equals(iDChain.OwnerName)).Ordinal] = iDChain.Parent.Id;

            while (reader.MoveToNextAttribute())
                attributes[columns.First(column => column.ColumnName.Equals(reader.Name) && column.Table.TableName.Equals(iDChain.OwnerName)).Ordinal] = reader.HasValue ? reader.Value : null;

            reader.Read();
            return new Row(iDChain.OwnerName, attributes);
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

        internal class Row
        {

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

            internal Row(string ownerName, object[] attributes)
            {
                this.ownerName = ownerName;
                this.attributes = attributes;
            }

        }

        private class IDChain
        {
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

            private readonly IDChain parent;

            internal IDChain Parent
            {
                get
                {
                    if (root) return this;
                    return parent;
                }
            }

            private readonly string ownerName;

            internal string OwnerName
            {
                get { return ownerName; }
            }

            private int id;

            internal int Id
            {
                get { return id; }
            }

            internal IDChain(IDChain parent, string ownerName, int depth)
            {
                parent.hasChildren = true;
                this.parent = parent;
                this.ownerName = ownerName;
                this.depth = depth;
                this.id = 0;
            }

            private IDChain()
            {
                this.root = true;
                this.depth = -1;
                this.ownerName = "root";
            }

            internal void PlusId()
            {
                this.id++;
            }

            internal static IDChain CreateRoot()
            {
                return new IDChain();
            }

        }

    }

}