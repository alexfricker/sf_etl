using M1SF.Services;
using System;

namespace M1SF
{
    class Program
    {
        private static CredentialManager credential;
        private static EtlManager etl;
        static void Main(string[] args)
        {
            try
            {
                Console.WriteLine(DateTime.Now.ToString() + ": Starting M1SF Data Service");
                credential = new CredentialManager();
                while (credential.GetStatus() != Models.StatusEnum.Started) { }

                etl = new EtlManager(credential);
                while (etl.GetStatus() != Models.StatusEnum.Started) { }
                Console.WriteLine();
            }
            catch(Exception e)
            {
                Console.WriteLine(e);
            }
        }
    }
}
