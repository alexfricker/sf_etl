using M1SF.Models;
using RestSharp;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;

namespace M1SF.Services
{
    public class CredentialManager
    {
        private string Username;
        private string Password;
        private string BaseUri;
        private string InstanceUri;
        private string Token;
        private Thread cmThread;
        private DateTime TokenRefresh;
        private StatusEnum Status;

        // Constructor
        public CredentialManager()
        {
            Console.WriteLine(DateTime.Now.ToString() + ": Creating Credential Manager Service");
            SetBaseCredentials();
            cmThread = new Thread(Start);
            Status = StatusEnum.Created;
            cmThread.Start();
        }

        private void SetBaseCredentials()
        {
            Console.WriteLine(DateTime.Now.ToString() + ": Retrieving credentials");
            Username = StaticResources.Username;
            Password = StaticResources.Password;
            BaseUri = StaticResources.BaseURI;
        }
        private IRestResponse GetLoginResponse()
        {
            return LoginAsync().Result;
        }
        private async Task<IRestResponse> LoginAsync()
        {
            var envelope = StaticResources.GetSfLoginEnvelope(Username, Password);
            var URL = StaticResources.GetSfLoginUri(BaseUri);
            var client = new RestClient(URL);
            var request = new RestRequest(Method.POST);

            client.Timeout = -1;
            request.AddHeader("Content-Type", "text/xml");
            request.AddHeader("SOAPAction", "login");
            request.AddParameter("text/xml", envelope, ParameterType.RequestBody);

            return await client.ExecuteAsync(request);
        }
        private async Task<IRestResponse> GetLoginResponseAsync()
        {
            return await LoginAsync();
        }
        private void SetToken(IRestResponse response)
        {
            XmlDocument xml = new XmlDocument();
            xml.LoadXml(response.Content);
            string serverUrl = (xml.GetElementsByTagName("serverUrl"))[0].InnerText;

            Token = (xml.GetElementsByTagName("sessionId"))[0].InnerText;            
            InstanceUri = serverUrl.Substring(0, serverUrl.IndexOf(".com") + 4);
            TokenRefresh = DateTime.Now;

            Console.WriteLine(DateTime.Now.ToString() + ": Refreshed Service Account token");
        }
        private void Start()
        {
            Console.WriteLine(DateTime.Now.ToString() + ": Starting Credential Manager Service");
            Status = StatusEnum.Starting;
            SetToken(GetLoginResponse());
            Status = StatusEnum.Started;
            Console.WriteLine(DateTime.Now.ToString() + ": Credential Manager Service started successfully");

            while (true)
            {
                if((DateTime.Now - TokenRefresh).TotalSeconds >= StaticResources.TokenRefreshSec)
                {
                    SetBaseCredentials();
                    SetToken(GetLoginResponse());
                }
            }
        }
        public void Stop()
        {
            cmThread.Abort();
        }
        public string GetToken()
        {
            return "Bearer " + Token;
        }
        public StatusEnum GetStatus()
        {
            return Status;
        }
        public string GetInstanceUri()
        {
            return InstanceUri;
        }
    }
}
