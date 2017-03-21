#include <stdio.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "omp.h"
#include "mpi.h"

//NUM_LINKS nao pode ser  maior que NUM_PAGINAS
#define NUM_PAGINAS 100000
#define NUM_LINKS_PAGINAS 20

typedef struct Pagina{
    int numPagina;
    int numLinks;
    int links[NUM_LINKS_PAGINAS];
}Pagina;




Pagina vetorPaginas[NUM_PAGINAS];
float vetorPageRank[NUM_PAGINAS];
int size, rank, sendcount;   
float total; 


void inicializarPageRank(){
	int i;
	for(i = 0 ; i < NUM_PAGINAS; i++){
        vetorPageRank[i] = 0.0;
	}
}

//gerando paginas
void gerarPaginas(){

 	int ini = ((rank) * (NUM_PAGINAS)/size);
 	int fim = (NUM_PAGINAS/size)*(rank + 1);
 	int i,j;
 	
 	printf("Processo: %d Inicio: %d Fim %d\n", rank, ini, fim );

 	
    srand( time(NULL) * rank );
    #pragma omp parallel for num_threads(omp_get_num_procs())
    for(i = ini ; i < fim; i++){
        
        vetorPaginas[i].numPagina = i;


        int links = rand()%NUM_LINKS_PAGINAS;
        vetorPaginas[i].numLinks = links;

        #pragma omp parallel for num_threads(omp_get_num_procs())
        for(j = 0; j < links; j++){
            int link = rand()%NUM_PAGINAS;
            while(link == i || existe(i,link,j)){
                link = rand()%NUM_PAGINAS;
            }
            vetorPaginas[i].links[j] = link;

        }
    }

}

//funcao utilizada na geracao de paginas, impedindo que uma pagina aponte para ela msm
int existe(int numPag, int link, int j){
    int i;
    for(i = 0; i< j; i++){
        if(vetorPaginas[numPag].links[i] == link)
            return 1;
    }
    return 0;
}


//funcao que imprimi as paginas com suas informaçoes
void imprimirPaginas(){
    int i,j;
    //printf("Paginas do processo: %d\n", rank);
    for(i = 0; i < NUM_PAGINAS; i++){
        printf("Pagina %d  links ->", vetorPaginas[i].numPagina);

        for(j = 0; j < vetorPaginas[i].numLinks; j++){
            printf("%d ", vetorPaginas[i].links[j]);
        }

        printf("\n");
    }
}

//funcao que imprimi o pageRank de todas as paginas
void imprimirPageRank(){
     int i;
    float total = 0.0;

    //#pragma omp parallel for  num_threads(omp_get_num_procs()) reduction(+:total)
    for(i = 0; i < NUM_PAGINAS; i++){
        printf("PageRank da pagina %d -> %f\n", i, vetorPageRank[i] );
        total += vetorPageRank[i];
    }

    printf("Total: %f\n", total);
}

//funcao que calcula o page rank das paginas
void calcularPageRank(){
 	int ini = (rank* (NUM_PAGINAS)/size);
 	int fim = ((NUM_PAGINAS/size)*(rank+1));
 	int i,j;
    float fatorInicial = 1.0/NUM_PAGINAS;
    #pragma omp parallel for num_threads(omp_get_num_procs())
   for(i = ini; i < fim; i++){

        //caso a pagina nao aponte para ninguem
        if(vetorPaginas[i].numLinks == 0){
            #pragma omp parallel for num_threads(omp_get_num_procs())
             for(j = 0; j < NUM_PAGINAS; j++){
                  if(j != i)
                       vetorPageRank[j] += fatorInicial/(NUM_PAGINAS-1);
             }
        }
        else{
            #pragma omp parallel for num_threads(omp_get_num_procs())
            for(j = 0; j < vetorPaginas[i].numLinks; j++){
                int pagAux = vetorPaginas[i].links[j];
               vetorPageRank[pagAux] += fatorInicial/vetorPaginas[i].numLinks;
            }
        }
    }


}


  
int main(int argc, char** argv)
{
    clock_t begin = clock();
    
  	int i,j;
  
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &size); 
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);	
    
    /*
    MPI_Datatype MPI_PAGINA;
    MPI_Datatype type[3] = { MPI_INT, MPI_INT, MPI_INT};
    int blocklen[3] = { 1, 1, NUM_PAGINAS};
    MPI_Aint disp[3];
    
    disp[0] = (char *)(&vetorPaginas[0].numPagina) - (char *)(&vetorPaginas[0]);
    disp[1] = (char *)(&vetorPaginas[0].numLinks) - (char *)(&vetorPaginas[0]);
    disp[2] = (char *)(&vetorPaginas[0].links) - (char *)(&vetorPaginas[0]);


    MPI_Type_struct(3, blocklen, disp, type, &MPI_PAGINA);
    MPI_Type_commit(&MPI_PAGINA);
    
    */

    inicializarPageRank();
    gerarPaginas();
    calcularPageRank();
    if(rank != 0){    
	    int ini = (rank* (NUM_PAGINAS)/size);
        int fim = ((NUM_PAGINAS/size)*(rank+1));
        int cont = 0;

        //enviando informaçoes das paginas para o processo 0
        for(i = ini; i < fim; i++){
            //numero da pagina
            MPI_Send(&vetorPaginas[i].numPagina, 1, MPI_INT, 0, cont, MPI_COMM_WORLD);
            cont++;

            //quantidade de links
            MPI_Send(&vetorPaginas[i].numLinks, 1, MPI_INT, 0, cont, MPI_COMM_WORLD);
            cont++;

            //vetor de links
            MPI_Send(&vetorPaginas[i].links, vetorPaginas[i].numLinks, MPI_INT, 0, cont, MPI_COMM_WORLD);
            cont++;
        }

        //enviando vetor do page rank
        MPI_Send(&vetorPageRank, NUM_PAGINAS, MPI_FLOAT, 0, rank + cont, MPI_COMM_WORLD);
            

    }

	if(rank == 0){
		   
           MPI_Status status;
           int contProcesso = 0, temp = 0;
           int source = NUM_PAGINAS/size;
           int numPagProc = 0; //variavel utilizada para o controle sobre o numero da tag de recebimento
	
        //recebendo numero de paginas dos demais processos
        for(i = source; i < NUM_PAGINAS; i++){
            contProcesso = temp;
            if(i % source == 0){
                contProcesso++;
                numPagProc = 0;
                temp = contProcesso;
            }
            //numero da Pagina
            MPI_Recv(&vetorPaginas[i].numPagina, 1, MPI_INT, contProcesso, numPagProc, MPI_COMM_WORLD, &status);
            numPagProc++;

            //quantidade de links da pagina
            MPI_Recv(&vetorPaginas[i].numLinks, 1, MPI_INT, contProcesso, numPagProc, MPI_COMM_WORLD, &status);
            numPagProc++;

            //vetor de links da pagina
            MPI_Recv(&vetorPaginas[i].links, vetorPaginas[i].numLinks, MPI_INT, contProcesso, numPagProc, MPI_COMM_WORLD, &status);
            numPagProc++;

        }



        //recebendo o pagerank calculado pelos demais processos
        for(i = 1; i < size; i++){

            // MPI_Reduce(&subtotal, &total,1, MPI_FLOAT, MPI_SUM, 0, MPI_COMM_WORLD);

            float vetAux[NUM_PAGINAS];
            MPI_Recv(&vetAux, NUM_PAGINAS, MPI_FLOAT, i, i + numPagProc, MPI_COMM_WORLD, &status);
            //printf("Enviado processo %d tag %d \n", i, i);
            for(j = 0; j < NUM_PAGINAS; j++){
                vetorPageRank[j] += vetAux[j];
            }

        } 
        
	}


    MPI_Finalize();

    clock_t end = clock();
    double time = (double) (end-begin)/CLOCKS_PER_SEC;

    //imprimindo resultado final
    if(rank == 0){
    	imprimirPaginas();	
    	imprimirPageRank();
        printf("Tempo: %f\n", time);
    }

    return 0;
}
