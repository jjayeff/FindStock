using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FindStock
{
    class Program
    {
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        // | Config                                                          |
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        private static Processor processor = new Processor();
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        // | Main Function                                                   |
        // = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
        static void Main(string[] args)
        {
            processor.Run();
        }
    }
}
