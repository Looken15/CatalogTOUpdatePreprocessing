using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace CatalogTOUpdatePreprocessing
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            openFileDialog1 = new OpenFileDialog();

        }

        private void openFileDialog1_FileOk(object sender, CancelEventArgs e)
        {

        }

        private void button1_Click(object sender, EventArgs e)
        {
            if (openFileDialog1.ShowDialog() == DialogResult.OK)
            {
                var sr = new StreamReader(openFileDialog1.FileName);
                if (openFileDialog1.FileName.Contains(".sql"))
                {
                    SQLPreprocessing(ref sr, openFileDialog1.FileName.Substring(0, openFileDialog1.FileName.ToList().FindLastIndex(x => x == '\\')));
                }
            }
        }

        public void Preprocessing(string line, string filename, bool append = false)
        {
            if (append)
            {
                File.AppendAllText(filename, ",(");
            }
            else
                File.WriteAllText(filename, "INSERT INTO 'gc_maintenance_vehicles' VALUES (");
            line = line.Remove(0, line.ToList().FindIndex(x => x == '(')).Remove(0, 1);
            line = line.Remove(line.Length - 2);
            var lines = line.Split(new string[] { "),(" }, StringSplitOptions.None);
            for (var i = 0; i < lines.Length; i++)
            {
                var tokens = Regex.Split(lines[i], ",(?=(?:[^\']*\'[^\']*\')*[^\']*$)");
                for (var j = 0; j < tokens.Length; j++)
                {
                    if (tokens[j] == "''")
                        tokens[j] = "' '";
                }
                var newLine = string.Join("@", tokens);
                File.AppendAllText(filename, newLine);
                if (i == lines.Length - 1)
                    File.AppendAllText(filename, ")");
                else
                    File.AppendAllText(filename, "),(");
            }
        }

        public void SQLPreprocessing(ref StreamReader sr, string path)
        {
            while (sr.Peek() >= 0)
            {
                var line = sr.ReadLine();
                while (!line.Contains("!40000"))
                {
                    line = sr.ReadLine();
                }
                line = sr.ReadLine();
                Preprocessing(line, path + "\\brandsPreprocessed.txt");
                line = sr.ReadLine(); line = sr.ReadLine();

                while (!line.Contains("!40000"))
                {
                    line = sr.ReadLine();
                }
                line = sr.ReadLine();
                Preprocessing(line, path + "\\detailsPreprocessed.txt");
                while (!line.Contains("!40000"))
                {
                    line = sr.ReadLine();
                    if (line.Contains("!40000")) break;
                    Preprocessing(line, path + "\\detailsPreprocessed.txt", true);
                }
                line = sr.ReadLine();
                while (!line.Contains("!40000"))
                {
                    line = sr.ReadLine();
                }
                while (!line.Contains("!40000"))
                {
                    line = sr.ReadLine();
                }
                line = sr.ReadLine(); line = sr.ReadLine(); line = sr.ReadLine();

                while (!line.Contains("!40000"))
                {
                    line = sr.ReadLine();
                }
                line = sr.ReadLine();
                Preprocessing(line, path + "\\modificationsPreprocessed.txt");
                line = sr.ReadLine(); line = sr.ReadLine();

                while (!line.Contains("!40000"))
                {
                    line = sr.ReadLine();
                }
                line = sr.ReadLine();
                Preprocessing(line, path + "\\imagesPreprocessed.txt");
                line = sr.ReadLine(); line = sr.ReadLine();

                while (!line.Contains("!40000"))
                {
                    line = sr.ReadLine();
                }
                line = sr.ReadLine();
                Preprocessing(line, path + "\\vehiclesPreprocessed.txt");
                line = sr.ReadLine();
                Preprocessing(line, path + "\\vehiclesPreprocessed.txt", true);

                return;
            }
        }
    }
}
