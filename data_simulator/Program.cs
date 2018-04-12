using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Newtonsoft.Json.Linq;

namespace datagentest
{
    class Program
    {
        private static EventHubClient eventHubClient;
        private const string EhConnectionString = "Endpoint=sb://myeventhub112233.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=mykey112233;EntityPath=myeventhub";
        private static int[] deviceIds = new int[] { 1, 2, 3 };
        private static int[] tags = new int[] { 1, 2, 3, 4, 5 };
        private static int[] minVals = new int[] { 10, 30, 60, 80, 100 };
        private static int[] maxVals = new int[] { 15, 40, 65, 85, 110 };


        private static int numMsgs = 100;
        private static int delay = 2000;


        public static void Main(string[] args)
        {
            while(true)
            {
                MainAsync(args).GetAwaiter().GetResult();
            }
        }

        private static async Task SendMessagesToEventHub(int numMessagesToSend)
        {

            for (var i = 0; i < numMessagesToSend; i++)
            {
                try
                {
                    var rnd = new Random();
                    var deviceId = deviceIds[rnd.Next(deviceIds.Length)];
                    var tagIdx = rnd.Next(tags.Length);
                    var tag = tags[tagIdx];
                    var val = Math.Round(minVals[tagIdx] + rnd.NextDouble() * (maxVals[tagIdx] - minVals[tagIdx]), 2);
                    var ts = DateTime.Now.ToUniversalTime();

                    var j = new JObject();
                    j["TS"] = ts;
                    j["Device"] = deviceId;
                    j["Tag"] = tag;
                    j["Value"] = val;

                    var message = j.ToString();
                    Console.WriteLine($"Sending message: {message}");
                    await eventHubClient.SendAsync(new EventData(Encoding.UTF8.GetBytes(message)));
                }
                catch (Exception exception)
                {
                    Console.WriteLine($"{DateTime.Now} > Exception: {exception.Message}");
                }

                await Task.Delay(delay);
            }

            Console.WriteLine($"{numMessagesToSend} messages sent.");
        }

        private static async Task MainAsync(string[] args)
        {
            eventHubClient = EventHubClient.CreateFromConnectionString(EhConnectionString);
            await SendMessagesToEventHub(numMsgs);
            await eventHubClient.CloseAsync();
        }
    }
}
