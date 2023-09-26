// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

using System.Collections.Generic;
using Newtonsoft.Json;

namespace Function.Domain.Models.Purview
{
       public class SparkProcessAttributes
    {
        [JsonProperty("name")]
        public string Name = "";
        [JsonProperty("qualifiedName")]
        public string QualifiedName = "";
        [JsonProperty("inputs")]
        public List<InputOutput>? Inputs = new List<InputOutput>();
        [JsonProperty("outputs")]
        public List<InputOutput>? Outputs = new List<InputOutput>();
        
        [JsonProperty("columnMapping")]
        public string ColumnMapping = "";
    }
}
