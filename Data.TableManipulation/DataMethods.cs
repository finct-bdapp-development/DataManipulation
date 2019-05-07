using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using Microsoft.Win32.SafeHandles;
using System.Runtime.InteropServices;

namespace Data.TableManipulation
{
    public class DataMethods : IDisposable
    {

        #region Dispose

        /// <summary>
        /// Flag: Has Dispose already been called
        /// </summary>
        bool disposed = false;

        //Safe handle instance
        SafeHandle handle = new SafeFileHandle(IntPtr.Zero, true);

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposed) return;
            if (disposing)
            {
                handle.Dispose();
            }
            disposed = true;
        }

        #endregion

        #region Change structure

        /// <summary>
        /// Changes the structure of a raw data file based on the settings file
        /// </summary>
        /// <param name="rawData">The raw data that will be converted</param>
        /// <param name="settingsPath">The full path including file name for the settings file</param>
        /// <returns></returns>
        public DataTable ConvertTableStructure(ref DataTable rawData, string settingsPath)
        {
            if(System.IO.File.Exists(settingsPath) == false)
            {
                throw new Exception("The settings file for reformatting the data could not be found.");
            }
            System.IO.StreamReader myReader = new System.IO.StreamReader(settingsPath);
            List<string> entries = new List<string>();
            while(myReader.EndOfStream == false)
            {
                string temp = myReader.ReadLine();
                entries.Add(temp);
            }
            return ConvertTableStructure(ref rawData, entries.ToArray());
        }

        /// <summary>
        /// Converts the structure of a raw data file based on the settings file
        /// </summary>
        /// <param name="rawData">The raw data that is to be converted</param>
        /// <param name="settings">An array of the settings that are to be applied</param>
        /// <returns></returns>
        public DataTable ConvertTableStructure(ref DataTable rawData, string[] settings)
        {
            DataTable temp = rawData.Copy();
            //TODO Run through the entries in the settings file and apply the relevant setting
            foreach(string item in settings)
            {
                string[] setting = item.Split(',');
                switch(setting[2])
                {
                    case "Rename":
                        RenameColumn(ref temp, setting);
                        break;
                    case "Counter":
                        AddCounterField(ref temp, setting);
                        break;
                    case "Insert":
                        AddNewField(ref temp, setting);
                        break;
                    case "Ignore":
                        RemoveColumn(ref temp, setting);
                        break;
                    case "OnlyRetain":
                        RemoveColumnsNotInArray(ref temp, setting);
                        break;
                    case "MergeFields":
                        string padChar = null;
                        if (string.IsNullOrEmpty(setting[6]) == false) padChar = setting[6];
                        MergeFields(ref temp, setting[3], setting[4], setting[5], padChar);
                        break;
                    case "OnlyIncludeMatch":
                        string includeField = setting[0];
                        List<string> includeValues = new List<string>(setting);
                        includeValues.RemoveAt(0);
                        includeValues.RemoveAt(0);
                        includeValues.RemoveAt(0);
                        ExcludeUnmatchedRows(ref temp, includeField, includeValues.ToArray());
                        break;
                    case "ExcludeMatches":
                        string excludeField = setting[0];
                        List<string> excludevalues = new List<string>(setting);
                        excludevalues.RemoveAt(0);
                        excludevalues.RemoveAt(0);
                        excludevalues.RemoveAt(0);
                        ExcludeMatchingRows(ref temp, excludeField, excludevalues.ToArray());
                        break;
                    default:
                        throw new Exception("The settings file includes an action (" + settings[2] + ") that is not currently supported.");
                }
            }
            return temp;
        }

        /// <summary>
        /// Changes the name of the field
        /// </summary>
        /// <param name="data">The table containing the original data\columns</param>
        /// <param name="setting">The settings (including the new name for the column)</param>
        private void RenameColumn(ref DataTable data, string[] setting)
        {
            RenameColumn(ref data, setting[0], setting[1]);
        }

        /// <summary>
        /// Changes the name of the field
        /// </summary>
        /// <param name="data">The table containing the original data</param>
        /// <param name="originalColumnName">The original name of the column</param>
        /// <param name="newColumnName">The new name of the column</param>
        private void RenameColumn(ref DataTable data, string originalColumnName, string newColumnName)
        {
            if (data.Columns.IndexOf(originalColumnName) == -1)
            {
                throw new Exception(originalColumnName + " is not a column in the supplied data.");
            }
            data.Columns[originalColumnName].ColumnName = newColumnName;
        }

        /// <summary>
        /// Inserts a counter field into the table 
        /// </summary>
        /// <param name="data">The table that the counter column will be added to</param>
        /// <param name="setting">The settings for the counter column</param>
        private void AddCounterField(ref DataTable data, string[] setting)
        {
            int seed = 1;
            if (string.IsNullOrEmpty(setting[4]) == false)
            seed = int.Parse(setting[4]);
            AddCounterField(ref data, setting[1], seed);
        }

        /// <summary>
        /// Inserts a counter field into the table
        /// </summary>
        /// <param name="data">The table that the counter column will be added to</param>
        /// <param name="fieldName">The name to be used for the counter field</param>
        /// <param name="seed">The starting value for counter field</param>
        private void AddCounterField(ref DataTable data, string fieldName, int seed)
        {
            if(data.Columns.IndexOf(fieldName) != -1)
            {
                throw new Exception("Cannot add a counter because there is already a field with the name " + fieldName + ".");
            }
            DataColumn col = new DataColumn(fieldName, typeof(Int32));
            col.AutoIncrement = true;
            col.AutoIncrementSeed = seed;
            col.AutoIncrementStep = 1;
            data.Columns.Add(col);
            if(data.Rows.Count != 0)
            {
                foreach(DataRow item in data.Rows)
                {
                    item[fieldName] = seed;
                    seed++;
                }
                data.AcceptChanges();
            }
        }

        /// <summary>
        /// Add a new column to the data table
        /// </summary>
        /// <param name="data">The table that the column will be added to</param>
        /// <param name="settings">The settings for the new column</param>
        private void AddNewField(ref DataTable data, string[] settings)
        {
            if(data.Columns.IndexOf(settings[1]) != -1)
            {
                throw new Exception("There is already a column with the name " + settings[1] + ".");
            }
            DataColumn col = new DataColumn(settings[1]);
            switch(settings[3])
            {
                case "Integer":
                    col.DataType = typeof(Int32);
                    break;
                case "Text":
                    col.DataType = typeof(String);
                    break;
                case "Double":
                    col.DataType = typeof(Double);
                    break;
                case "DateTime":
                    col.DataType = typeof(DateTime);
                    break;
                default:
                    throw new Exception(settings[3] + " is not currently supported when adding a new field.");
            }
            if(string.IsNullOrEmpty(settings[4])==false)
            {
                col.DefaultValue = settings[4];
            }
            data.Columns.Add(col);
        }

        /// <summary>
        /// Deletes a column and its data from the data table
        /// </summary>
        /// <param name="data">The table the column will be removed from</param>
        /// <param name="settings">The settings showing which column is to be removed</param>
        private void RemoveColumn(ref DataTable data, string[] settings)
        {
            RemoveColumn(ref data, settings[0]);
        }

        /// <summary>
        /// Deletes a column and its data from the data table
        /// </summary>
        /// <param name="data">The table the column will be removed from</param>
        /// <param name="fieldName">The name of the column which is to be removed</param>
        /// <remarks>An error will not be thrown if the specified field is not part of the table</remarks>
        private void RemoveColumn(ref DataTable data, string fieldName)
        {
            if(data.Columns.IndexOf(fieldName) != -1)
            {
                data.Columns.Remove(fieldName);
            }
        }

        /// <summary>
        /// Removes any columns in the data table that are not in the string array
        /// </summary>
        /// <param name="data">The data to be manipulated</param>
        /// <param name="fieldsToRetain">An array containing the fields to be retained</param>
        private void RemoveColumnsNotInArray(ref DataTable data, string[] fieldsToRetain)
        {
            for(int counter = 0; counter < data.Columns.Count; counter++)
            {
                if(fieldsToRetain.Contains(data.Columns[counter].ColumnName) == false)
                {
                    data.Columns.Remove(data.Columns[counter].ColumnName);
                    counter--;
                }
            }
            data.AcceptChanges();
        }

        /// <summary>
        /// Merges the data in two fields into a single field
        /// </summary>
        /// <param name="data">The data to be converted</param>
        /// <param name="field1">The name of the first field being merged</param>
        /// <param name="field2">The name of the second field being merged</param>
        /// <param name="newFieldName">The name of the field that the data will be merged into - if the field
        /// does not exist it will be added to the table</param>
        /// <param name="pad">Any character that should be placed between the joined fields</param>
        /// <remarks>Where the data is being merged into an existing field newFieldName must equal field2</remarks>
        private void MergeFields(ref DataTable data, string field1, string field2, string newFieldName, string pad)
        {
            if(data.Columns.IndexOf(newFieldName) == -1)
            {
                DataColumn col = new DataColumn(newFieldName, typeof(string));
                data.Columns.Add(col);
                foreach(DataRow item in data.Rows)
                {
                    item[newFieldName] = item[field1].ToString() + pad + item[field2].ToString();
                }
                data.AcceptChanges();
            }
            else
            {
                if(newFieldName != field2)
                {
                    throw new Exception("Data fields could not be merged into an existing column which is not one of the fields being merged.");
                }
                foreach(DataRow item in data.Rows)
                {
                    item[field2] = item[field1].ToString() + pad + item[field2].ToString();
                }
                data.AcceptChanges();
            }
        }

        /// <summary>
        /// Removes a data row where the value stored in it does not match a value in the array
        /// </summary>
        /// <param name="data">The data that will be manipulated</param>
        /// <param name="fieldName">The field that will be checked for matches</param>
        /// <param name="valuesToMatch">An array of the values that should be retained</param>
        private void ExcludeUnmatchedRows(ref DataTable data, string fieldName, string[] valuesToMatch)
        {
            for(int counter = 0;counter < data.Rows.Count;counter++)
            {
                if(valuesToMatch.Contains(data.Rows[counter][fieldName].ToString()) == false)
                {
                    data.Rows.RemoveAt(counter);
                    counter--;
                }
            }
            data.AcceptChanges();
        }

        /// <summary>
        /// Remove any row where the data in the specified field matches the values to be excluded
        /// </summary>
        /// <param name="data">The data to be manipulated</param>
        /// <param name="fieldName">The field that will be checked for matches</param>
        /// <param name="valuesToExclude">An array of the values to be excluded</param>
        private void ExcludeMatchingRows(ref DataTable data, string fieldName, string[] valuesToExclude)
        {
            for(int counter = 0; counter< data.Rows.Count; counter++)
            {
                if(valuesToExclude.Contains(data.Rows[counter][fieldName].ToString()) == true)
                {
                    data.Rows.RemoveAt(counter);
                    counter--;
                }
            }
        }

        #endregion

    }

}
