using Microsoft.Azure.KeyVault;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Configuration.AzureKeyVault;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;
using System.IO;

namespace AzureFunctionApp
{
    public class Program
    {
        public static void Main()
        {
            var host = new HostBuilder()
               .ConfigureFunctionsWorkerDefaults()
               .ConfigureAppConfiguration(config => config
                    .SetBasePath(Directory.GetCurrentDirectory())
                    .AddJsonFile("appsettings.json")
                    .AddEnvironmentVariables())
               .ConfigureAppConfiguration((context, config) =>
               {
                   var settings = config.Build();
                   if (settings["Environment"] == "Production")
                   {
                       var keyVaultEndpoint = settings["AzureKeyVaultEndpoint"];
                       if (!string.IsNullOrEmpty(keyVaultEndpoint))
                       {
                           var azureServiceTokenProvider = new AzureServiceTokenProvider();
                           var keyVaultClient = new KeyVaultClient(new KeyVaultClient.AuthenticationCallback(azureServiceTokenProvider.KeyVaultTokenCallback));
                           config.AddAzureKeyVault(keyVaultEndpoint, keyVaultClient, new DefaultKeyVaultSecretManager());
                       }
                   }                   
               })
               .Build();

            host.Run();
        }
    }
}