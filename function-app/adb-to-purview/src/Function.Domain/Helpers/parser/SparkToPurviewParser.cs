// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using Function.Domain.Helpers;
using Function.Domain.Models.Settings;
using Function.Domain.Models.OL;
using Function.Domain.Models.Adb;
using Function.Domain.Models.Purview;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using Newtonsoft.Json;
using System.Text.RegularExpressions;

namespace Function.Domain.Helpers
{
    /// <summary>
    /// Creates Purview Spark objects from OpenLineage and ADB data from the jobs API
    /// </summary>
    public class SparkToPurviewParser
    {
        private readonly ILogger _logger;
        private readonly ILoggerFactory _loggerFactory;
        private readonly ParserSettings _parserConfig;
        private readonly IQnParser _qnParser;
        private readonly IColParser _colParser;
        private readonly EnrichedEvent _eEvent;
        // private readonly string _adbWorkspaceUrl;
        const string SETTINGS = "OlToPurviewMappings";
        // Regex ADF_JOB_NAME_REGEX = new Regex(@"^ADF_(.*)_(.*)_(.*)_(.*)$", RegexOptions.Compiled );

        /// <summary>
        /// Constructor for SparkToPurviewParser
        /// </summary>
        /// <param name="loggerFactory">Loggerfactory from Function framework DI</param>
        /// <param name="configuration">Configuration from Function framework DI</param>
        /// <param name="eEvent">The enriched event which combines OpenLineage data with data from ADB get job API</param>
        public SparkToPurviewParser(ILoggerFactory loggerFactory, IConfiguration configuration, EnrichedEvent eEvent)
        {
            _logger = loggerFactory.CreateLogger<SparkToPurviewParser>();
            _loggerFactory = loggerFactory;

            _parserConfig = new ParserSettings();

            // try{
            // var map = configuration[SETTINGS];
            // // _parserConfig = JsonConvert.DeserializeObject<ParserSettings>(map) ?? throw new MissingCriticalDataException("critical config not found");
            // } 
            // catch (Exception ex) {
            //     _logger.LogError(ex,"DatabricksToPurviewParser: Error retrieving ParserSettings.  Please make sure these are configured on your function.");
            //     throw;
            // }
            // if (_parserConfig is null || eEvent.OlEvent?.Run.Facets.EnvironmentProperties is null)
            // {
            //     var ex = new MissingCriticalDataException("DatabricksToPurviewParser: Missing critical data.  Please make sure your OlToPurviewMappings configuration is correct.");
            //     _logger.LogError(ex, ex.Message);
            //     throw ex;
            // }
            _eEvent = eEvent;
            // _adbWorkspaceUrl = _eEvent.OlEvent.Job.Namespace.Split('#')[0];
            // _parserConfig.AdbWorkspaceUrl = this.GetDatabricksWorkspace().Attributes.Name;
            _qnParser = new QnParser(_parserConfig, _loggerFactory);

            _colParser = new ColParser(_parserConfig, _loggerFactory,
                                      _eEvent.OlEvent,
                                      _qnParser);

            // _adbWorkspaceUrl = _eEvent.OlEvent.Job.Namespace.Split('#')[0];

        }

        /// <summary>
        /// Gets the job type from the supported ADB job types.  Currently all are supported except Spark Submit jobs.
        /// </summary>
        /// <returns></returns>
        public JobType GetJobType()
        {
          return JobType.InteractiveNotebook;
            // if (_eEvent.AdbRoot?.JobTasks?[0] == null)
            // {
            //     return JobType.InteractiveNotebook;
            // }
            // else if (_eEvent.AdbRoot?.JobTasks[0].NotebookTask != null)
            // {
            //     return JobType.JobNotebook;
            // }
            // else if (_eEvent.AdbRoot?.JobTasks[0].SparkPythonTask != null)
            // {
            //     return JobType.JobPython;
            // }
            // else if (_eEvent.AdbRoot?.JobTasks[0].PythonWheelTask != null)
            // {
            //     return JobType.JobWheel;
            // }
            // else if (_eEvent.AdbRoot?.JobTasks[0].SparkJarTask != null)
            // {
            //     return JobType.JobJar;
            // }
            // return JobType.Unsupported;
        }

        /// <summary>
        /// Creates a Purview Databricks workspace object for an enriched event
        /// </summary>
        /// <returns>A Databricks workspace object</returns>
        // public DatabricksWorkspace GetDatabricksWorkspace()
        // {
        //     DatabricksWorkspace databricksWorkspace = new DatabricksWorkspace();
        //     databricksWorkspace.Attributes.Name = $"{_adbWorkspaceUrl}.azuredatabricks.net";
        //     databricksWorkspace.Attributes.QualifiedName = $"databricks://{_adbWorkspaceUrl}.azuredatabricks.net";
            
        //     return databricksWorkspace;
        // }


        public SparkApplication GetSparkApplication()
        {
          SparkApplication sparkApplication = new SparkApplication();
          sparkApplication.Attributes.Name = $"{_eEvent.OlEvent.Job.Namespace}";
          sparkApplication.Attributes.QualifiedName = $"{_eEvent.OlEvent.Job.Namespace}";
          return sparkApplication;
        }

        /// <summary>
        /// Creates a Spark process object from an enriched event
        /// </summary>
        /// <returns>A Spark process object</returns>
        public SparkProcess GetSparkProcess(string appQn)
        {

            var sparkProcess = new SparkProcess();
            // sparkProcess.Attributes.Name = $"{_eEvent.OlEvent.Job.Namespace}_{_eEvent.OlEvent.Run.RunId}";
            // sparkProcess.Attributes.QualifiedName = $"{_eEvent.OlEvent.Job.Namespace}_{_eEvent.OlEvent.Run.RunId}";
            //var ColumnAttributes = new ColumnLevelAttributes();

            var inputs = new List<InputOutput>();
            foreach (IInputsOutputs input in _eEvent.OlEvent!.Inputs)
            {
                inputs.Add(GetInputOutputs(input));
            }

            var outputs = new List<InputOutput>();
            var outputName = _eEvent.OlEvent.Run.RunId;
            foreach (IInputsOutputs output in _eEvent.OlEvent!.Outputs)
            {
                outputs.Add(GetInputOutputs(output));
                outputName = output.Name.ToLower().Replace("/", "_").Replace(".", "_");
            }

            sparkProcess.Attributes.Inputs = inputs;
            sparkProcess.Attributes.Outputs = outputs;
            sparkProcess.RelationshipAttributes.Application.QualifiedName = appQn;
            sparkProcess.Attributes.Name = $"{_eEvent.OlEvent.Job.Namespace}_{outputName}";
            sparkProcess.Attributes.QualifiedName = $"{_eEvent.OlEvent.Job.Namespace}_{outputName}";
            return sparkProcess;
        }

        // public List<SparkColumnLineage> GetAllSparkColumnLineage(string processQn) {
        //     List<SparkColumnLineage> sparkColumnLineageList = new List<SparkColumnLineage>();

        //     foreach(Outputs olOutput in _eEvent.OlEvent.Outputs)
        //     {   
        //         // colName: col Lineage
        //         foreach(KeyValuePair<string, ColumnLineageInputFieldClass> colInfo in olOutput.Facets.ColFacets.fields)
        //         {
        //             InputOutput purviewOutput = new InputOutput();
        //             purviewOutput.TypeName = "spark_column";
        //         }
        //     }

        //     return sparkColumnLineageList;
        // }


        // private SparkColumnLineage GetSparkColumnLineage(string processQn, string colName, ){
        //   var sparkColumnLineage = new SparkColumnLineage();
        //   sparkColumnLineage.Attributes.Name = $"{_eEvent.OlEvent.Job.Namespace}_{_eEvent.OlEvent.Run.RunId}";
        //   sparkColumnLineage.Attributes.QualifiedName = $"{_eEvent.OlEvent.Job.Namespace}_{_eEvent.OlEvent.Run.RunId}";
        //   sparkColumnLineage.RelationshipAttributes.Process.QualifiedName = processQn;
        //   return sparkColumnLineage;
        // }

        
        // private DatabricksProcessAttributes GetProcAttributes(string taskQn, List<InputOutput> inputs, List<InputOutput> outputs, Event sparkEvent)
        // {
        //     var pa = new DatabricksProcessAttributes();
        //     pa.Name = sparkEvent.Run.Facets.EnvironmentProperties!.EnvironmentProperties.SparkDatabricksNotebookPath + sparkEvent.Outputs[0].Name;
        //     pa.QualifiedName = $"{taskQn}/processes/{GetInputsOutputsHash(inputs, outputs)}";
        //     pa.ColumnMapping = JsonConvert.SerializeObject(_colParser.GetColIdentifiers());
        //     pa.SparkPlan = sparkEvent.Run.Facets.SparkLogicalPlan.ToString(Formatting.None);
        //     pa.Inputs = inputs;
        //     pa.Outputs = outputs;
        //     return pa;
        // }

        private InputOutput GetInputOutputs(IInputsOutputs inOut)
        {
            var id = _qnParser.GetIdentifiers(inOut.NameSpace,inOut.Name);
            var inputOutputId = new InputOutput();
            inputOutputId.TypeName = id.PurviewType;
            inputOutputId.UniqueAttributes.QualifiedName = id.QualifiedName;

            return inputOutputId;
        }

        private string GetInputsOutputsHash(List<InputOutput> inputs, List<InputOutput> outputs)
        {
            inputs.Sort((x, y) => x.UniqueAttributes.QualifiedName.CompareTo(y.UniqueAttributes.QualifiedName));;
            StringBuilder sInputs = new StringBuilder(inputs.Count);
            foreach (var input in inputs)
            {
                sInputs.Append(input.UniqueAttributes.QualifiedName.ToLower().ToString());
                if (!input.Equals(inputs.Last()))
                {
                    sInputs.Append(",");
                }
            }
            var inputHash = GenerateMd5Hash(sInputs.ToString());
            // Outputs should only ever have one item
            var outputHash = GenerateMd5Hash(outputs[0].UniqueAttributes.QualifiedName.ToString());

            return $"{inputHash}->{outputHash}";
        }

        private string GenerateMd5Hash(string input)
        {
            byte[] tmpSource;
            byte[] tmpHash;

            //Create a byte array from source data.
            tmpSource = ASCIIEncoding.ASCII.GetBytes(input);

            //Compute hash based on source data.
            tmpHash = MD5.Create().ComputeHash(tmpSource);

            StringBuilder sOutput = new StringBuilder(tmpHash.Length);
            for (int i=0;i < tmpHash.Length; i++)
            {
                sOutput.Append(tmpHash[i].ToString("X2"));
            }
            return sOutput.ToString();
        }

        private string TruncateAdfTaskName(string inputName){
            // Return ADF_factoryName_pipelineName_notebookName portions
            string[] job_name_parts = inputName.Split("_");
            string[] job_name_except_last_element = job_name_parts.Take(job_name_parts.Count() - 1).ToArray();
            return string.Join("_", job_name_except_last_element);
        }
        private string TruncateAdfJobName(string inputName){
            // Return ADF_factoryName_pipelineName portions
            string[] job_name_parts = inputName.Split("_");
            string[] job_name_except_last_element = job_name_parts.Take(job_name_parts.Count() - 2).ToArray();
            return string.Join("_", job_name_except_last_element);
        }

        public IColParser GetColumnParser()
        {
            return this._colParser;
        }
    }
}
