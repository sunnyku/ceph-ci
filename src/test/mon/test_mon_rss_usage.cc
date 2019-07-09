#include <algorithm>
#include <iostream>
#include <fstream>
#include <string>
#include <numeric>
#include <regex>
#include <cmath>
#include <system_error>

using namespace std;

int main()
{
  cout << "Mon RSS Usage Test" << endl;

  string target_directory("/var/log/ceph/");
  string filePath = target_directory + "ceph-mon-rss-usage.log";
  ifstream buffer(filePath.c_str());
  string line;
  vector<unsigned long> results;
  while(getline(buffer, line) && !line.empty()) {
    string rssUsage;
    size_t pos = line.find(':');
    if (pos != string::npos) {
      rssUsage = line.substr(0, pos);
    }
    if (!rssUsage.empty()) {
      results.push_back(stoul(rssUsage));
    }
  }

  if (results.empty()) {
    cout << "Error: No grep results found!" << endl;
    exit(ENOENT);
  }

  auto maxe = *(max_element(results.begin(), results.end()));
  cout << "Stats for mon RSS Memory Usage:" << endl;
  cout << "Parsed " << results.size() << " entries." << endl;
  cout << "Max: " << maxe << endl;
  cout << "Min: " << *(min_element(results.begin(), results.end())) << endl;
  auto sum = accumulate(results.begin(), results.end(),
                        static_cast<unsigned long long>(0));
  auto mean = sum / results.size();
  cout << "Mean average: " << mean << endl;
  vector<unsigned long> diff(results.size());
  transform(results.begin(), results.end(), diff.begin(),
            [mean](unsigned long x) { return x - mean; });
  auto sump = inner_product(diff.begin(), diff.end(), diff.begin(), 0.0);
  auto stdev = sqrt(sump / results.size());
  cout << fixed <<  "Standard deviation: " << stdev << endl;

  cout << "Completed successfully" << endl;
  return 0;
}
