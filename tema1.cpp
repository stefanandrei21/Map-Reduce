//Panait Stefan-Andrei 332CA
#include <stdio.h>
#include <vector>
#include <algorithm>
#include <iostream>   
#include <fstream> 
#include <stdlib.h>
#include <pthread.h>
#include <math.h>
#include <set>

using namespace std;

vector<string> nume_fisiere;
int number_of_reducer;
int number_of_maper;
vector<vector<vector<int>>> list_exponent;
int max_pow;

pthread_mutex_t mutex;
pthread_barrier_t barrier;


void *mapper(void *arg)
{
	int tid = *(int *) arg;
	
 	while(nume_fisiere.size() > 0 ){
		//mutex pentru a nu accesa sau a da pop in acelasti timp pe diferite threaduri la un element din vector
		pthread_mutex_lock(&mutex);
		if(nume_fisiere.size() < 1) break;
			int nr;
			ifstream ind(nume_fisiere.back());
			nume_fisiere.pop_back();
			pthread_mutex_unlock(&mutex);
			ind >> nr;
			for(int i = 0; i < nr; i++){
				int number_to_find;
				ind >> number_to_find;
				//binary search
				for(int k = 2; k < max_pow + 1; k++) {

					long long left = 1;
					long long right = sqrt(number_to_find);
					bool ok = 0;
					while (left <= right) {
						long long mid = left + (right - left) / 2;
						if(pow(mid, k) == number_to_find){
							left = right + 1;
							ok = 1;
							continue;
						}
						if(pow(mid, k) < number_to_find) {
							left = mid + 1;
							continue;
						} 
							right = mid - 1;
					
					}
					if (ok == true) {
						list_exponent[tid][k - 2].push_back(number_to_find);
					}
					
					
				}
				if(number_to_find == 1){
					for(int i = 2; i < max_pow + 1; i++){
						list_exponent[tid][i - 2].push_back(number_to_find);
					}
				}
				
		}
		ind.close();
		
 	}
	//bariera pentru a astepta ca toate threadurile de map sa se termine inainte de a incepe threadurile de reducers
	pthread_barrier_wait(&barrier);
	pthread_exit(NULL);
}

void* reducer(void *arg) {
	int tid = *(int *) arg;
	//aceasta bariera asteapta ca threadurile de map sa se terima (aceeasi functionalitate ca cea de mai sus)
	pthread_barrier_wait(&barrier);
	string filename;
	filename +="out" + to_string(tid + 2) + ".txt";
	ofstream file(filename);
	set<int> numbers;

	for(int i = 0; i < number_of_maper; i++){
		for(auto x : list_exponent[i][tid])
			numbers.insert(x);
	}
	
	file << numbers.size();
	file.close();
	pthread_exit(NULL);
}

int main(int argc, char *argv[])
{
	

	
	 if (argc < 4) {
        cout << ("Parametrii nu sunt dati corect\n");
        exit(-1);
    }
	ifstream read(argv[3]);
	
	number_of_maper = atoi(argv[1]);
	number_of_reducer = atoi(argv[2]);
	//citesc fisierele si retin
	int nr_fisiere;
	read >> nr_fisiere;
	string nume;
	for(int i = 0; i < nr_fisiere; i++){
		read >> nume;
		nume_fisiere.push_back(nume);
	}
	read.close();


	max_pow = number_of_reducer + 1;

	//initializez mutex si bariera

	pthread_mutex_init(&mutex, NULL);
	pthread_barrier_init(&barrier, NULL, number_of_maper + number_of_reducer);

	int m_r = number_of_maper + number_of_reducer;
	list_exponent.resize(number_of_maper);
	for(int i = 0; i < number_of_maper; i++){
		list_exponent[i].resize(number_of_reducer + 1);
	}
	pthread_t mapping_thread[number_of_maper];
	pthread_t reducing_thread[number_of_reducer];
	
	int tid[number_of_maper];
	int tid_r[number_of_reducer];
	//creez threadurile
	for(int i = 0; i < m_r; i++) {
		if(i < number_of_maper) {
			tid[i] = i;
			int r = pthread_create(&mapping_thread[i], NULL, mapper, &tid[i]);
			if (r) {
				cout << "Nu s-au putut creea threadurile pt mappers" << endl;
			}
		} else {
			int k = i - number_of_maper;
			tid_r[k] = k;
			int r = pthread_create(&reducing_thread[k], NULL, reducer, &tid_r[k]);
			if (r) {
				cout << "Nu s-au putut creea threadurile pt reducers" << endl;
			}
		 }
		
	
	}
	//join la map si reduce
	for(int i = 0; i < m_r; i++){
		if(i < number_of_maper) {
			int r = pthread_join(mapping_thread[i], NULL);
			if (r) {
			cout << "Nu s-a putut da join la mappers" << endl;
			}
		} else {
			int r = pthread_join(reducing_thread[i - number_of_maper], NULL);
			if (r) {
			cout << "Nu s-a putut da join la reducers" << endl;
			}
		 }
	}

	
	//destroy mutex si bariera
	pthread_mutex_destroy(&mutex);
	pthread_barrier_destroy(&barrier);
	return 0;
}
