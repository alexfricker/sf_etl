using M1SF.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace M1SF.Services
{
    public class EtlManager
    {
        private readonly CredentialManager credential;
        private string TargetConnection;
        private List<EtlInstance> etlInstances;
        private StatusEnum Status;
        private Thread etlThread;
        public EtlManager(CredentialManager cm)
        {
            Console.WriteLine(DateTime.Now.ToString() + ": Creating ETL Manager Service");
            credential = cm;
            etlInstances = new List<EtlInstance>();
            etlThread = new Thread(Start);
            Status = StatusEnum.Created;
            etlThread.Start();
        }

        private void SetTargetConnection()
        {
            TargetConnection = StaticResources.DestinationDb;
            Console.WriteLine(DateTime.Now.ToString() + ": Refreshed target database connection");
        }
        private void SetInstances()
        {
            // TODO: Check that object exists (SchemaManager.GetSobjectList(), etc
            var TargetInstances = StaticResources.GetSObjects();

            foreach(var item in etlInstances)
            {
                var target = TargetInstances.Where(t => t == item.SObject).FirstOrDefault();
                if(target == null)
                {
                    item.Stop();
                }
            }
            foreach(var item in TargetInstances)
            {
                var instance = etlInstances.Where(t => t.SObject == item).FirstOrDefault();
                if (instance == null)
                {
                    etlInstances.Add(new EtlInstance(item, this, credential, DateTime.Parse("1900-01-01")));
                }
            }
        }
        public void Start()
        {
            Console.WriteLine(DateTime.Now.ToString() + ": Starting ETL Manager Service");
            Status = StatusEnum.Starting;
            SetTargetConnection();
            SchemaManager.SyncTargetSchemas(TargetConnection);
            SetInstances();
            Status = StatusEnum.Started;
            Console.WriteLine(DateTime.Now.ToString() + ": ETL Manager Service started successfully");

            while (true)
            {
                SetInstances();
            }
        }
        public void Stop()
        {

        }
        public StatusEnum GetStatus()
        {
            return Status;
        }
        public string GetTargetConnection()
        {
            return TargetConnection;
        }
    }
}
