/************************************************************
	Project#1:	CLP & DDL
 ************************************************************/

#include "db.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <limits.h>
#include <vector>
#include <algorithm>
#include <set>

using namespace std;
#if defined(_WIN32) || defined(_WIN64)
#define strcasecmp _stricmp
#endif
int main(int argc, char** argv)
{
	int rc = 0;
	token_list *tok_list=NULL, *tok_ptr=NULL, *tmp_tok_ptr=NULL;

	if ((argc != 2) || (strlen(argv[1]) == 0))
	{
		printf("Usage: db \"command statement\"\n");
		return 1;
	}

	rc = initialize_tpd_list();

  if (rc)
  {
		printf("\nError in initialize_tpd_list().\nrc = %d\n", rc);
  }
	else
	{
    rc = get_token(argv[1], &tok_list);

		/* Test code */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
			printf("%16s \t%d \t %d\n",tok_ptr->tok_string, tok_ptr->tok_class,
				      tok_ptr->tok_value);
			tok_ptr = tok_ptr->next;
		}
    
		if (!rc)
		{
			rc = do_semantic(tok_list);
		}

		if (rc)
		{
			tok_ptr = tok_list;
			while (tok_ptr != NULL)
			{
				if ((tok_ptr->tok_class == error) ||
					  (tok_ptr->tok_value == INVALID))
				{
					printf("\nError in the string: %s\n", tok_ptr->tok_string);
					printf("rc=%d\n", rc);
					break;
				}
				tok_ptr = tok_ptr->next;
			}
		}

    /* Whether the token list is valid or not, we need to free the memory */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
      tmp_tok_ptr = tok_ptr->next;
      free(tok_ptr);
      tok_ptr=tmp_tok_ptr;
		}
	}

	return rc;
}

/************************************************************* 
	This is a lexical analyzer for simple SQL statements
 *************************************************************/
int get_token(char* command, token_list** tok_list)
{
	int rc=0,i,j;
	char *start, *cur, temp_string[MAX_TOK_LEN];
	bool done = false;
	
	start = cur = command;
	while (!done)
	{
		bool found_keyword = false;

		/* This is the TOP Level for each token */
	  memset ((void*)temp_string, '\0', MAX_TOK_LEN);
		i = 0;

		/* Get rid of all the leading blanks */
		while (*cur == ' ')
			cur++;

		if (cur && isalpha(*cur))
		{
			// find valid identifier
			int t_class;
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((isalnum(*cur)) || (*cur == '_'));

			if (!(strchr(STRING_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				   is not a blank, (, ), or a comma, then append this
					 character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{

				// We have an identifier with at least 1 character
				// Now check if this ident is a keyword
				for (j = 0, found_keyword = false; j < TOTAL_KEYWORDS_PLUS_TYPE_NAMES; j++)
				{
					if ((strcasecmp(keyword_table[j], temp_string) == 0))
					{
						found_keyword = true;
						break;
					}
				}

				if (found_keyword)
				{
				  if (KEYWORD_OFFSET+j < K_CREATE)
						t_class = type_name;
					else if (KEYWORD_OFFSET+j >= F_SUM)
            t_class = function_name;
          else
					  t_class = keyword;

					add_to_list(tok_list, temp_string, t_class, KEYWORD_OFFSET+j);
				}
				else
				{
					if (strlen(temp_string) <= MAX_IDENT_LEN)
					  add_to_list(tok_list, temp_string, identifier, IDENT);
					else
					{
						add_to_list(tok_list, temp_string, error, INVALID);
						rc = INVALID;
						done = true;
					}
				}

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if (isdigit(*cur))
		{
			// find valid number
			do 
			{
				temp_string[i++] = *cur++;
			}
			while (isdigit(*cur));

			if (!(strchr(NUMBER_BREAK, *cur)))
			{
				/* If the next char following the keyword or identifier
				   is not a blank or a ), then append this
					 character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{
				add_to_list(tok_list, temp_string, constant, INT_LITERAL);

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}
		else if ((*cur == '(') || (*cur == ')') || (*cur == ',') || (*cur == '*')
		         || (*cur == '=') || (*cur == '<') || (*cur == '>') || (*cur == '.'))
		{
			/* Catch all the symbols here. Note: no look ahead here. */
			int t_value;
			switch (*cur)
			{
				case '(' : t_value = S_LEFT_PAREN; break;
				case ')' : t_value = S_RIGHT_PAREN; break;
				case ',' : t_value = S_COMMA; break;
				case '*' : t_value = S_STAR; break;
				case '=' : t_value = S_EQUAL; break;
				case '<' : t_value = S_LESS; break;
				case '>' : t_value = S_GREATER; break;
                case '.' : t_value = S_DOT; break;
			}

			temp_string[i++] = *cur++;

			add_to_list(tok_list, temp_string, symbol, t_value);

			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
		}
    else if (*cur == '\'')
    {
      /* Find STRING_LITERRAL */
			int t_class;
      cur++;
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((*cur) && (*cur != '\''));

      temp_string[i] = '\0';

			if (!*cur)
			{
				/* If we reach the end of line */
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
      else /* must be a ' */
      {
        add_to_list(tok_list, temp_string, constant, STRING_LITERAL);
        cur++;
				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
        }
      }
    }
		else
		{
			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
			else
			{
				/* not a ident, number, or valid symbol */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
		}
	}
			
  return rc;
}

void add_to_list(token_list **tok_list, char *tmp, int t_class, int t_value)
{
	token_list *cur = *tok_list;
	token_list *ptr = NULL;

	// printf("%16s \t%d \t %d\n",tmp, t_class, t_value);

	ptr = (token_list*)calloc(1, sizeof(token_list));
	strcpy(ptr->tok_string, tmp);
	ptr->tok_class = t_class;
	ptr->tok_value = t_value;
	ptr->next = NULL;

  if (cur == NULL)
		*tok_list = ptr;
	else
	{
		while (cur->next != NULL)
			cur = cur->next;

		cur->next = ptr;
	}
	return;
}

int do_semantic(token_list *tok_list)
{
	int rc = 0, cur_cmd = INVALID_STATEMENT;
	bool unique = false;
  token_list *cur = tok_list;

	if ((cur->tok_value == K_CREATE) &&
			((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("CREATE TABLE statement\n");
		cur_cmd = CREATE_TABLE;
		cur = cur->next->next;
	}
    else if((cur->tok_value == K_INSERT) &&
            ((cur->next != NULL) && (cur->next->tok_value == K_INTO)))
    {
        printf("INSERT INTO statement\n");
        cur_cmd = INSERT;
        cur = cur->next->next;
    }
	else if ((cur->tok_value == K_DROP) &&
					((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("DROP TABLE statement\n");
		cur_cmd = DROP_TABLE;
		cur = cur->next->next;
	}
    else if ((cur->tok_value == K_DELETE) &&
                    ((cur->next != NULL) && (cur->next->tok_value == K_FROM)))
    {
        printf("DELETE TABLE statement\n");
        cur_cmd = DELETE;
        cur = cur->next->next;
    }
    else if (cur->tok_value == K_UPDATE)
    {
        printf("UPDATE TABLE statement\n");
        cur_cmd = UPDATE;
        cur = cur->next;
    }
	else if ((cur->tok_value == K_LIST) &&
					((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("LIST TABLE statement\n");
		cur_cmd = LIST_TABLE;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_LIST) &&
					((cur->next != NULL) && (cur->next->tok_value == K_SCHEMA)))
	{
		printf("LIST SCHEMA statement\n");
		cur_cmd = LIST_SCHEMA;
		cur = cur->next->next;
	}
    else if ((cur->tok_value == K_SELECT))
    {
        printf("SELECT statement\n");
        cur_cmd = SELECT;
        cur = cur->next;
    }
	else
  {
		printf("Invalid statement\n");
		rc = cur_cmd;
	}

	if (cur_cmd != INVALID_STATEMENT)
	{
		switch(cur_cmd)
		{
			case CREATE_TABLE:
						rc = sem_create_table(cur);
						break;
            case DELETE:
                        rc = sem_delete_table(cur);
                        break;
            case UPDATE:
                        rc = sem_update_table(cur);
                        break;
            case INSERT:
                        rc = sem_insert_into(cur);
                        break;
            case SELECT:
                        rc = sem_select(cur);
                        break;
			case DROP_TABLE:
						rc = sem_drop_table(cur);
						break;
			case LIST_TABLE:
						rc = sem_list_tables();
						break;
			case LIST_SCHEMA:
						rc = sem_list_schema(cur);
						break;
			default:
					; /* no action */
		}
	}
	
	return rc;
}

int sem_create_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry tab_entry;
	tpd_entry *new_entry = NULL;
	bool column_done = false;
	int cur_id = 0;
	cd_entry	col_entry[MAX_NUM_COL];


	memset(&tab_entry, '\0', sizeof(tpd_entry));
	cur = t_list;
	if ((cur->tok_class != keyword) &&
		  (cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if ((new_entry = get_tpd_from_list(cur->tok_string)) != NULL)
		{
			rc = DUPLICATE_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			strcpy(tab_entry.table_name, cur->tok_string);
			cur = cur->next;
			if (cur->tok_value != S_LEFT_PAREN)
			{
				//Error
				rc = INVALID_TABLE_DEFINITION;
				cur->tok_value = INVALID;
			}
			else
			{
				memset(&col_entry, '\0', (MAX_NUM_COL * sizeof(cd_entry)));

				/* Now build a set of column entries */
				cur = cur->next;
				do
				{
					if ((cur->tok_class != keyword) &&
							(cur->tok_class != identifier) &&
							(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_COLUMN_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						int i;
						for(i = 0; i < cur_id; i++)
						{
              /* make column name case sensitive */
							if (strcmp(col_entry[i].col_name, cur->tok_string)==0)
							{
								rc = DUPLICATE_COLUMN_NAME;
								cur->tok_value = INVALID;
								break;
							}
						}

						if (!rc)
						{
							strcpy(col_entry[cur_id].col_name, cur->tok_string);
							col_entry[cur_id].col_id = cur_id;
							col_entry[cur_id].not_null = false;    /* set default */

							cur = cur->next;
							if (cur->tok_class != type_name)
							{
								// Error
								rc = INVALID_TYPE_NAME;
								cur->tok_value = INVALID;
							}
							else
							{
                /* Set the column type here, int or char */
								col_entry[cur_id].col_type = cur->tok_value;
								cur = cur->next;
		
								if (col_entry[cur_id].col_type == T_INT)
								{
									if ((cur->tok_value != S_COMMA) &&
										  (cur->tok_value != K_NOT) &&
										  (cur->tok_value != S_RIGHT_PAREN))
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
								  else
									{
										col_entry[cur_id].col_len = sizeof(int);
										
										if ((cur->tok_value == K_NOT) &&
											  (cur->next->tok_value != K_NULL))
										{
											rc = INVALID_COLUMN_DEFINITION;
											cur->tok_value = INVALID;
										}	
										else if ((cur->tok_value == K_NOT) &&
											    (cur->next->tok_value == K_NULL))
										{					
											col_entry[cur_id].not_null = true;
											cur = cur->next->next;
										}
	
										if (!rc)
										{
											/* I must have either a comma or right paren */
											if ((cur->tok_value != S_RIGHT_PAREN) &&
												  (cur->tok_value != S_COMMA))
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
		                  {
												if (cur->tok_value == S_RIGHT_PAREN)
												{
 													column_done = true;
												}
												cur = cur->next;
											}
										}
									}
								}   // end of T_INT processing
								else
								{
									// It must be char() or varchar() 
									if (cur->tok_value != S_LEFT_PAREN)
									{
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
									else
									{
										/* Enter char(n) processing */
										cur = cur->next;
		
										if (cur->tok_value != INT_LITERAL)
										{
											rc = INVALID_COLUMN_LENGTH;
											cur->tok_value = INVALID;
										}
										else
										{
											/* Got a valid integer - convert */
											col_entry[cur_id].col_len = atoi(cur->tok_string);
											cur = cur->next;
											
											if (cur->tok_value != S_RIGHT_PAREN)
											{
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
											{
												cur = cur->next;
						
												if ((cur->tok_value != S_COMMA) &&
														(cur->tok_value != K_NOT) &&
														(cur->tok_value != S_RIGHT_PAREN))
												{
													rc = INVALID_COLUMN_DEFINITION;
													cur->tok_value = INVALID;
												}
												else
												{
													if ((cur->tok_value == K_NOT) &&
														  (cur->next->tok_value != K_NULL))
													{
														rc = INVALID_COLUMN_DEFINITION;
														cur->tok_value = INVALID;
													}
													else if ((cur->tok_value == K_NOT) &&
																	 (cur->next->tok_value == K_NULL))
													{					
														col_entry[cur_id].not_null = true;
														cur = cur->next->next;
													}
		
													if (!rc)
													{
														/* I must have either a comma or right paren */
														if ((cur->tok_value != S_RIGHT_PAREN) &&															  (cur->tok_value != S_COMMA))
														{
															rc = INVALID_COLUMN_DEFINITION;
															cur->tok_value = INVALID;
														}
														else
													  {
															if (cur->tok_value == S_RIGHT_PAREN)
															{
																column_done = true;
															}
															cur = cur->next;
														}
													}
												}
											}
										}	/* end char(n) processing */
									}
								} /* end char processing */
							}
						}  // duplicate column name
					} // invalid column name

					/* If rc=0, then get ready for the next column */
					if (!rc)
					{
						cur_id++;
					}

				} while ((rc == 0) && (!column_done));
	
				if ((column_done) && (cur->tok_value != EOC))
				{
					rc = INVALID_TABLE_DEFINITION;
					cur->tok_value = INVALID;
				}

				if (!rc)
				{
					/* Now finished building tpd and add it to the tpd list */
					tab_entry.num_columns = cur_id;
					tab_entry.tpd_size = sizeof(tpd_entry) + 
															 sizeof(cd_entry) *	tab_entry.num_columns;
				  tab_entry.cd_offset = sizeof(tpd_entry);
					new_entry = (tpd_entry*)calloc(1, tab_entry.tpd_size);

					if (new_entry == NULL)
					{
						rc = MEMORY_ERROR;
					}
					else
					{
						memcpy((void*)new_entry,
							     (void*)&tab_entry,
									 sizeof(tpd_entry));
		
						memcpy((void*)((char*)new_entry + sizeof(tpd_entry)),
									 (void*)col_entry,
									 sizeof(cd_entry) * tab_entry.num_columns);
	
						rc = add_tpd_to_list(new_entry);

						free(new_entry);
					}
				}
			}
		}
	}
    int rc1 = 0,i,size,remainder,temp=0,rec_size;
    char str[10];
    FILE *fhandle = NULL;
//    struct _stat file_stat;
    char file_name[MAX_IDENT_LEN+4];
    table_file_header *new_header;
    table_file_header *table_header;
    strcpy(file_name,tab_entry.table_name);
    strcat(file_name,".tab");
    if((fhandle = fopen(file_name, "wbc")) == NULL)
    {
        rc1 = FILE_OPEN_ERROR;
    }
else
    {
        for(i=0;i<tab_entry.num_columns;i++)
        {
            temp = temp + col_entry[i].col_len;
        }
        size = temp + tab_entry.num_columns;
        remainder = size % 4;
            if (remainder == 0)
            { rec_size = size;}
            else
            { rec_size = size + 4 - remainder;}
        table_header = NULL;
        table_header = (table_file_header*)malloc((100 * rec_size)+sizeof(table_file_header));
        new_header = (table_file_header*)malloc((100 * rec_size)+sizeof(table_file_header));
        
        if (!table_header)
        {
            rc1 = MEMORY_ERROR;
        }
        else
        {
            table_header->num_records =0;
            table_header->file_header_flag=0;
            table_header->record_offset = sizeof(table_file_header);
            table_header->file_size = sizeof(table_file_header);
            table_header->tpd_ptr = 0;
            table_header->record_size = rec_size;
            memcpy((void*)new_header,
                     (void*)table_header,
                         sizeof(table_file_header));
            fwrite(new_header, sizeof(table_file_header), 1, fhandle);
            fflush(fhandle);
            fclose(fhandle);
    }
}

  return rc;
}
int sem_delete_table(token_list *t_list)
{
    int rc=0,j,rel,check_value,copy_size=0;
    tpd_entry *temp_entry =NULL;
    tpd_entry *new_entry = NULL;
    cd_entry  *col_entry = NULL;
    char tab_name[MAX_IDENT_LEN+4];
    char column_name[MAX_IDENT_LEN+4];
    token_list *cur;
    cur = t_list;
    bool found = false;
    bool isnull = false;
    bool isnotnull = false;
    bool isnot = false;
    int num_of_deleted_records = 0;
    
    FILE *fhandle = NULL;
    table_file_header *header_and_contents_from_file=NULL;
    if ((cur->tok_class != keyword) &&
          (cur->tok_class != identifier) &&
            (cur->tok_class != type_name))
    {
        rc = INVALID_TABLE_NAME;
        cur->tok_value = INVALID;
    }
    else if ((new_entry = get_tpd_from_list(cur->tok_string)) == NULL)
        {
            rc = TABLE_NOT_EXIST;
            cur->tok_value = INVALID;
        }
        else
        {
            strcpy(tab_name,cur->tok_string);
            cur= cur->next;
            
           if(cur->tok_value != EOC)
           {
            if(cur->tok_value != K_WHERE)
            {
                rc = INVALID_SYNTAX;
                cur->tok_value = INVALID;
            }
            else
            {
                
                if(cur->next->tok_value == K_NOT)
                {
                    isnot = true;
                    cur = cur->next;
                }
                cur = cur->next;
                temp_entry=get_tpd_from_list(tab_name);
                col_entry = (cd_entry*)((char*)temp_entry + temp_entry->cd_offset);
                for(int i=0;i<temp_entry->num_columns;i++)
                {
                    if(strcasecmp(col_entry[i].col_name,cur->tok_string)==0)
                    {
                        found = true;
                        j=i;
                    }
                }
                if(!found)
                {
                    rc = INVALID_COLUMN_NAME;
                    cur->tok_value = INVALID;
                }
                else
                {
                    strcpy(column_name,cur->tok_string);
                    cur = cur->next;
                    if(cur->tok_value != S_LESS && cur->tok_value != S_GREATER && cur->tok_value != S_EQUAL && cur->tok_value != K_IS)
                    {
                        rc = INVALID_RELATIONAL_OPERATOR ;
                        cur->tok_value = INVALID;
                        return rc;
                    }
                    else
                    {
                        
                        if(cur->tok_value == K_IS)
                        {
                            cur = cur->next;
                            if(cur->tok_value!= K_NOT && cur->tok_value!= K_NULL)
                                {
                                    rc =INVALID_SYNTAX;
                                    cur->tok_value = INVALID;
                                    return rc;
                                }
                                else
                                    {
                                        if(cur->tok_value == K_NOT)
                                            {
                                                cur = cur->next;
                                                if(cur->tok_value!= K_NULL)
                                                    {
                                                        rc =INVALID_SYNTAX;
                                                        cur->tok_value = INVALID;
                                                        return rc;
                                                    }
                                                else
                                                    {
                                                        isnotnull = true;
                                                        
                                                    }
                                            }
                                        else
                                            {
                                                isnull = true;
                                                if(col_entry[j].not_null == 1 && isnull == true)
                                                    {
                                                        rc = NULL_VALUE_INSERTED;
                                                        cur->tok_value = INVALID;
                                                        return rc;
                                                    }
                                                
                                                
                                            }
                                    }
                                if (cur->next->tok_value != EOC)
                                {
                                    rc = INVALID_STATEMENT;
                                    cur->next->tok_value = INVALID;
                                    return rc;
                                }
                            
                            strcat(tab_name,".tab");
                            fhandle = fopen(tab_name, "rbc");
                            table_file_header* temp_header = NULL;
                            temp_header = (table_file_header*)calloc(1, sizeof(table_file_header));
                            fread(temp_header,sizeof(table_file_header), 1, fhandle);
                            fflush(fhandle);
                            fclose(fhandle);
                            
                            fhandle = fopen(tab_name, "rbc");
                            header_and_contents_from_file = (table_file_header*)malloc((100 * temp_header->record_size)+ sizeof(table_file_header));
                            fread(header_and_contents_from_file,temp_header->file_size, 1, fhandle);
                            fflush(fhandle);
                            fclose(fhandle);
                            
                            int col_offset = 0;
                            for (int i  = 0; i < j; i++){
                                col_offset = col_offset + col_entry[i].col_len +  1;
                            }
                            
                            int extra = col_offset;
                            
                            for ( int cur_record = 0; cur_record < temp_header->num_records; cur_record++ ){

                                int* size = (int*)malloc(1);
                                memcpy(size, (int*)((char*)header_and_contents_from_file+sizeof(table_file_header)+extra), 1);
                                extra++;
                                
                                if ( isnull && *size == 0){
                                    memmove((char*)header_and_contents_from_file+sizeof(table_file_header) + extra - 1 - col_offset, (char*)header_and_contents_from_file+sizeof(table_file_header) + extra - 1 - col_offset + temp_header->record_size, (100 * temp_header->record_size)+ sizeof(table_file_header));
                                    extra = extra - (temp_header->record_size);
                                    num_of_deleted_records++;
                                    
                                    
                                }
                                
                                else if ( isnotnull && *size != 0){
                                    memmove((char*)header_and_contents_from_file+sizeof(table_file_header) + extra - 1 - col_offset, (char*)header_and_contents_from_file+sizeof(table_file_header) + extra - 1 - col_offset + temp_header->record_size, (100 * temp_header->record_size)+ sizeof(table_file_header));
                                    extra = extra - (temp_header->record_size);
                                    num_of_deleted_records++;
                                    
                                    
                                }
                                extra = extra + temp_header->record_size - 1;
                            }
                            
                            
                            }
                       else
                       {
                        rel = cur->tok_value;
                        cur = cur->next;
                        if(col_entry[j].col_type == T_INT && cur->tok_value != INT_LITERAL && cur->tok_value != K_NULL)
                        {
                            printf("Data type mismatch in Where Clause");
                            rc = DATA_MISMATCH;
                            cur->tok_value = INVALID;
                            return rc;
                        }
                        else if(col_entry[j].col_type == T_CHAR && cur->tok_value != STRING_LITERAL && cur->tok_value != K_NULL)
                        {
                            printf("Data type mismatch in Where Clause");
                            rc = DATA_MISMATCH;
                            cur->tok_value = INVALID;
                            return rc;
                        }
                        else
                        {
                            if(cur->tok_value == INT_LITERAL)
                            {
                                if(atoi(cur->tok_string)<0)
                                {
                                    rc = INVALID_COLUMN_ENTRY;
                                    cur->tok_value = INVALID;
                                    return rc;
                                }
                                else
                                {
                                
                                }
                            }
                            else
                            {
                                copy_size = strlen(cur->tok_string);
                                if(copy_size > col_entry[j].col_len )
                                {
                                    rc = INVALID_COLUMN_ENTRY;
                                    cur->tok_value = INVALID;
                                    return rc;
                                }
                                else
                                {
                                }
                            }
                            strcat(tab_name,".tab");
                            fhandle = fopen(tab_name, "rbc");
                            table_file_header* temp_header = NULL;
                            temp_header = (table_file_header*)calloc(1, sizeof(table_file_header));
                            fread(temp_header,sizeof(table_file_header), 1, fhandle);
                            fflush(fhandle);
                            fclose(fhandle);
                            
                            fhandle = fopen(tab_name, "rbc");
                            header_and_contents_from_file = (table_file_header*)malloc((100 * temp_header->record_size)+ sizeof(table_file_header));
                            fread(header_and_contents_from_file,temp_header->file_size, 1, fhandle);
                            fflush(fhandle);
                            fclose(fhandle);
                            int col_offset = 0;
                            for (int i  = 0; i < j; i++){
                                col_offset = col_offset + col_entry[i].col_len +  1;
                            }
                            
                            if(col_entry[j].col_type == T_INT)
                            {

                                
                                int extra = col_offset;
                                for ( int cur_record = 0; cur_record < temp_header->num_records; cur_record++ ){

                                    int* size = (int*)malloc(1);
                                    memcpy(size, (int*)((char*)header_and_contents_from_file+sizeof(table_file_header)+extra), 1);
                                    extra++;
                                    int* data = (int*)malloc(sizeof(int));
                                    memcpy(data, (int*)((char*)header_and_contents_from_file+sizeof(table_file_header)+extra), *size);
                                    if (rel == S_EQUAL)
                                    {
                                        if (cur->tok_value == K_NULL && *size == 0){
                                            memmove((char*)header_and_contents_from_file+sizeof(table_file_header) + extra - 1 - col_offset, (char*)header_and_contents_from_file+sizeof(table_file_header) + extra - 1 - col_offset + temp_header->record_size, (100 * temp_header->record_size)+ sizeof(table_file_header));
                                            extra = extra - (temp_header->record_size);
                                            num_of_deleted_records++;
                                            
                                        }
                                        else if (*data == atoi(cur->tok_string) && cur->tok_value != K_NULL && *size != 0){
                                            memmove((char*)header_and_contents_from_file+sizeof(table_file_header) + extra - 1 - col_offset, (char*)header_and_contents_from_file+sizeof(table_file_header) + extra - 1 - col_offset + temp_header->record_size, (100 * temp_header->record_size)+ sizeof(table_file_header));
                                            extra = extra - (temp_header->record_size);
                                            num_of_deleted_records++;
                                        }
                                    }
                                    if (rel == S_GREATER)
                                    {
                                        if (*data > atoi(cur->tok_string) && cur->tok_value != K_NULL && *size != 0){
                                            memmove((char*)header_and_contents_from_file+sizeof(table_file_header) + extra - 1 - col_offset, (char*)header_and_contents_from_file+sizeof(table_file_header) + extra - 1 - col_offset + temp_header->record_size, (100 * temp_header->record_size)+ sizeof(table_file_header));
                                            extra = extra - (temp_header->record_size);
                                            num_of_deleted_records++;
                                        }
                                    }
                                    if (rel == S_LESS)
                                    {
                                        if (*data < atoi(cur->tok_string) && cur->tok_value != K_NULL && *size != 0){
                                            memmove((char*)header_and_contents_from_file+sizeof(table_file_header) + extra - 1 - col_offset, (char*)header_and_contents_from_file+sizeof(table_file_header) + extra - 1 - col_offset + temp_header->record_size, (100 * temp_header->record_size)+ sizeof(table_file_header));
                                            extra = extra - (temp_header->record_size);
                                            num_of_deleted_records++;
                                        }
                                    }
                                    
                                    extra = extra + temp_header->record_size - 1;
                                
                                }

                            }
                            else{

                                int extra = col_offset;
                                for ( int cur_record = 0; cur_record < temp_header->num_records; cur_record++ ){
                                    
                                    int* size = (int*)malloc(1);
                                    memcpy(size, (int*)((char*)header_and_contents_from_file+sizeof(table_file_header)+extra), 1);
                                    extra++;
                                    char data[*size];
                                    memcpy(data, (char*)((char*)header_and_contents_from_file+sizeof(table_file_header)+extra), *size);
                                    data[*size] = '\0';
                                    
                                    if (rel == S_EQUAL)
                                    {
                                        if (cur->tok_value == K_NULL && *size == 0){
                                            memmove((char*)header_and_contents_from_file+sizeof(table_file_header) + extra - 1 - col_offset, (char*)header_and_contents_from_file+sizeof(table_file_header) + extra - 1 - col_offset + temp_header->record_size, (100 * temp_header->record_size)+ sizeof(table_file_header));
                                            extra = extra - (temp_header->record_size);
                                            num_of_deleted_records++;
                                            
                                        }
                                        else if (strcmp(data,cur->tok_string)==0 && cur->tok_value != K_NULL && *size != 0){
                                            memmove((char*)header_and_contents_from_file+sizeof(table_file_header) + extra - 1 - col_offset, (char*)header_and_contents_from_file+sizeof(table_file_header) + extra - 1 - col_offset + temp_header->record_size, (100 * temp_header->record_size)+ sizeof(table_file_header));
                                            extra = extra - (temp_header->record_size);
                                            num_of_deleted_records++;
                                            
                                        }
                                    }
                                    
                                    if (rel == S_GREATER)
                                    {
                                        if (strcmp(data,cur->tok_string)>0 && cur->tok_value != K_NULL && *size != 0){
                                            memmove((char*)header_and_contents_from_file+sizeof(table_file_header) + extra - 1 - col_offset, (char*)header_and_contents_from_file+sizeof(table_file_header) + extra - 1 - col_offset + temp_header->record_size, (100 * temp_header->record_size)+ sizeof(table_file_header));
                                            extra = extra - (temp_header->record_size);
                                            num_of_deleted_records++;
                                        }
                                    }
                                    if (rel == S_LESS)
                                    {
                                        if (strcmp(data,cur->tok_string)<0 && cur->tok_value != K_NULL && *size != 0){
                                            memmove((char*)header_and_contents_from_file+sizeof(table_file_header) + extra - 1 - col_offset, (char*)header_and_contents_from_file+sizeof(table_file_header) + extra - 1 - col_offset + temp_header->record_size, (100 * temp_header->record_size)+ sizeof(table_file_header));
                                            extra = extra - (temp_header->record_size);
                                            num_of_deleted_records++;
                                        }
                                    }
                                
                                    extra = extra + temp_header->record_size - 1;
                                
                                }
                            }
                            
                        }
                           if (cur->next->tok_value != EOC)
                           {
                               rc = INVALID_STATEMENT;
                               cur->next->tok_value = INVALID;
                               return rc;
                           }
                }
                }
                }
            }
           }
           else {
               strcat(tab_name,".tab");
               fhandle = fopen(tab_name, "rbc");
               table_file_header* temp_header = NULL;
               temp_header = (table_file_header*)calloc(1, sizeof(table_file_header));
               fread(temp_header,sizeof(table_file_header), 1, fhandle);
               fflush(fhandle);
               fclose(fhandle);
               
               
               fhandle = fopen(tab_name, "rbc");
               header_and_contents_from_file = (table_file_header*)malloc((100 * temp_header->record_size)+ sizeof(table_file_header));
               fread(header_and_contents_from_file,temp_header->file_size, 1, fhandle);
               fflush(fhandle);
               fclose(fhandle);
               
               
               memset((char*)header_and_contents_from_file + sizeof(table_file_header), '\0', header_and_contents_from_file->record_size * header_and_contents_from_file->num_records);
               num_of_deleted_records = header_and_contents_from_file->num_records;
           }

            header_and_contents_from_file->num_records = header_and_contents_from_file->num_records - num_of_deleted_records;
            header_and_contents_from_file->file_size = header_and_contents_from_file->file_size - (num_of_deleted_records*header_and_contents_from_file->record_size);

            fhandle = fopen(tab_name, "wbc");
            fwrite(header_and_contents_from_file, ((sizeof(table_file_header)+header_and_contents_from_file->record_size * header_and_contents_from_file->num_records)), 1, fhandle);
            fflush(fhandle);
            fclose(fhandle);
            
        }
   if(num_of_deleted_records == 0)
   {
       printf("No record found\n");
   }
   else if(num_of_deleted_records == 1)
   {
       printf("1 record deleted\n");
   }
    else
   {
       printf("%d records deleted\n",num_of_deleted_records);
       
   }
    printf("The size of %s is : %d\n",tab_name,header_and_contents_from_file->file_size);
    return rc;
}
int sem_update_table(token_list *t_list)
{
    int rc = 0,j,val,new_value,copy_size=0,k,rel,copy_size1=0;
    tpd_entry *temp_entry =NULL;
    tpd_entry *new_entry = NULL;
    cd_entry  *col_entry = NULL;
    token_list *cur;
    cur = t_list;
   // FILE *fhandle = NULL;
    bool found = false;
    bool found_1 = false;
    char tab_name[MAX_IDENT_LEN+4];
    char column_name[MAX_IDENT_LEN+4];
    char check_column_name[MAX_IDENT_LEN+4];
    bool isnull = false;
    bool isnotnull = false;
    int records_updated = 0;
    bool isnot = false;
    
    FILE *fhandle = NULL;
    table_file_header *header_and_contents_from_file=NULL;
    char str[255];
    
   // char file_name[MAX_IDENT_LEN+4];
    if ((cur->tok_class != keyword) &&
          (cur->tok_class != identifier) &&
            (cur->tok_class != type_name))
    {
        rc = INVALID_TABLE_NAME;
        cur->tok_value = INVALID;
        return rc;
    }
    
      else if ((new_entry = get_tpd_from_list(cur->tok_string)) == NULL)
        {

            rc = TABLE_NOT_EXIST;
            cur->tok_value = INVALID;
            return rc;
        }
        else
        {
            strcpy(tab_name,cur->tok_string);
            cur=cur->next;
            if (cur->tok_value != K_SET)
            {
                //Error
                rc = INVALID_SYNTAX;
                cur->tok_value = INVALID;
                return rc;
            }
            else
            {   cur=cur->next;
                temp_entry=get_tpd_from_list(tab_name);
                col_entry = (cd_entry*)((char*)temp_entry + temp_entry->cd_offset);
                for(int i=0;i<temp_entry->num_columns;i++)
                {
                    if(strcasecmp(col_entry[i].col_name,cur->tok_string)==0)
                    {
                        found = true;
                        j=i;
                    }
                }
                if(!found)
                {
                    rc = INVALID_COLUMN_NAME;
                    cur->tok_value = INVALID;
                    return rc;
                }
                else
                {
                    strcpy(column_name,cur->tok_string);
                    cur = cur->next;
                    if(cur->tok_value != S_EQUAL)
                    {
                        rc = INVALID_SYNTAX;
                        cur->tok_value = INVALID;
                        return rc;
                    }
                    else
                    {
                        cur= cur->next;
                       // val=cur->tok_value;
                        if(col_entry[j].col_type == T_INT && cur->tok_value != INT_LITERAL && cur->tok_value != K_NULL)
                        {
                            rc = DATA_MISMATCH;
                            cur->tok_value = INVALID;
                            return rc;
                        }
                        else if(col_entry[j].col_type == T_CHAR && cur->tok_value != STRING_LITERAL && cur->tok_value != K_NULL)
                        {
                            rc = DATA_MISMATCH;
                            cur->tok_value = INVALID;
                            return rc;
                        }
                        else if(col_entry[j].not_null == 1 && cur->tok_value == K_NULL)
                        {
                            rc = NULL_VALUE_INSERTED;
                            cur->tok_value = INVALID;
                            return rc;
                        }
                        else
                        {
                            // -1 if null
                            // 0 if int
                            // 1 if char
                            int is_int = 0;
                                                        
                            strcat(tab_name,".tab");
                            fhandle = fopen(tab_name, "rbc");
                            table_file_header* temp_header = NULL;
                            temp_header = (table_file_header*)calloc(1, sizeof(table_file_header));
                            fread(temp_header,sizeof(table_file_header), 1, fhandle);
                            fflush(fhandle);
                            fclose(fhandle);
                            
                            fhandle = fopen(tab_name, "rbc");
                            header_and_contents_from_file = (table_file_header*)malloc((100 * temp_header->record_size)+ sizeof(table_file_header));
                            fread(header_and_contents_from_file,temp_header->file_size, 1, fhandle);
                            fflush(fhandle);
                            fclose(fhandle);
                            
                            int col_offset = 0;
                            int check_offset = 0;
                            for (int i  = 0; i < j; i++){
                                col_offset = col_offset + col_entry[i].col_len +  1;
                            }
                            
                            if(cur->tok_value == INT_LITERAL)
                            {
                                if(atoi(cur->tok_string)<0)
                                {
                                    rc = INVALID_COLUMN_ENTRY;
                                    cur->tok_value = INVALID;
                                    return rc;
                                }
                                else
                                {
                                    is_int = 0;
                                    new_value = atoi(cur->tok_string);
                                }
                            }
                            else if (cur->tok_value == K_NULL ){
                                is_int = -1;
                            }
                            else
                            {
                                is_int = 1;
                                copy_size = strlen(cur->tok_string);
                                if(copy_size > col_entry[j].col_len )
                                {
                                    rc = INVALID_COLUMN_ENTRY;
                                    cur->tok_value = INVALID;
                                    return rc;
                                }
                                else
                                {
                                strcpy(str,cur->tok_string);
                                }
                            }
                            
                            
                           if(cur->next->tok_value != EOC)
                           {
                               cur=cur->next;
                               if(cur->tok_value != K_WHERE)
                               {
                                   rc = INVALID_SYNTAX;
                                   cur->tok_value = INVALID;
                                   return rc;
                               }
                               else
                               {
                                   if(cur->next->tok_value == K_NOT)
                                   {
                                       isnot = true;
                                       cur = cur->next;
                                   }
                                   cur = cur->next;
                                   for(int i=0;i<temp_entry->num_columns;i++)
                                   {
                                       if(strcasecmp(col_entry[i].col_name,cur->tok_string)==0)
                                       {
                                           found_1 = true;
                                           k=i;
                                       }
                                   }
                                   if(!found_1)
                                   {
                                       rc = INVALID_COLUMN_NAME;
                                       cur->tok_value = INVALID;
                                       return rc;
                                   }
                                   else
                                   {
                                       strcpy(check_column_name,cur->tok_string);
                                       cur = cur->next;
                                       if(cur->tok_value != S_LESS && cur->tok_value != S_GREATER && cur->tok_value != S_EQUAL && cur->tok_value != K_IS)
                                       {
                                           rc = INVALID_RELATIONAL_OPERATOR;
                                           cur->tok_value = INVALID;
                                           return rc;
                                       }
                                       else
                                       {
                                           if(cur->tok_value==K_IS)
                                           {
                                                   cur = cur->next;
                                                   if(cur->tok_value!= K_NOT && cur->tok_value!= K_NULL)
                                                   {
                                                       rc =INVALID_SYNTAX;
                                                       cur->tok_value = INVALID;
                                                       return rc;
                                                   }
                                                   else
                                                   {
                                                       if(cur->tok_value == K_NOT)
                                                       {
                                                           cur = cur->next;
                                                           if(cur->tok_value!= K_NULL)
                                                           {
                                                               rc =INVALID_SYNTAX;
                                                               cur->tok_value = INVALID;
                                                               return rc;
                                                           }
                                                           else
                                                           {
                                                               isnotnull = true;
                                                           }
                                                       }
                                                      else if(cur->tok_value == K_NULL)
                                                       {
                                                           isnull = true;
                                                           if(col_entry[k].not_null == 1 && isnull == true)
                                                           {
                                                               rc = NULL_VALUE_INSERTED;
                                                               cur->tok_value = INVALID;
                                                               return rc;
                                                           }
                                                       }
                                                   }
                                               if (cur->next->tok_value != EOC)
                                               {
                                                   rc = INVALID_STATEMENT;
                                                   cur->next->tok_value = INVALID;
                                                   return rc;
                                               }
                                               }
                                          
                                           else
                                           {
                                           rel = cur->tok_value;
                                           cur = cur->next;
                                           if (cur->next->tok_value != EOC)
                                           {
                                               rc = INVALID_STATEMENT;
                                               cur->next->tok_value = INVALID;
                                               return rc;
                                           }
                                           if(col_entry[k].col_type == T_INT && cur->tok_value != INT_LITERAL && cur->tok_value != K_NULL)
                                           {
                                               printf("Data type mismatch in Where Clause");
                                               rc = DATA_MISMATCH;
                                               cur->tok_value = INVALID;
                                               return rc;
                                           }
                                           else if(col_entry[k].col_type == T_CHAR && cur->tok_value != STRING_LITERAL && cur->tok_value != K_NULL)
                                           {
                                               printf("Data type mismatch in Where Clause");
                                               rc = DATA_MISMATCH;
                                               cur->tok_value = INVALID;
                                               return rc;
                                           }
                                           else if(col_entry[k].not_null == 1 && cur->tok_value == K_NULL)
                                           {
                                               rc = NULL_VALUE_INSERTED;
                                               cur->tok_value = INVALID;
                                               return rc;
                                           }
                                           else
                                           {
                                               if(cur->tok_value == INT_LITERAL)
                                               {
                                                   if(atoi(cur->tok_string)<0)
                                                   {
                                                       rc = INVALID_COLUMN_ENTRY;
                                                       cur->tok_value = INVALID;
                                                       return rc;
                                                   }
                                                   else
                                                   {
                                                   
                                                   }
                                               }
                                               else if(cur->tok_value == K_NULL)
                                               {
                                                   
                                               }
                                               else if(cur->tok_value == STRING_LITERAL)
                                               {
                                                   copy_size1 = strlen(cur->tok_string);
                                                   if(copy_size1 > col_entry[k].col_len )
                                                   {
                                                       rc = INVALID_COLUMN_ENTRY;
                                                       cur->tok_value = INVALID;
                                                       return rc;
                                                   }
                                                   else
                                                   {
                                                   }
                                               }
                                           }
                                           }
                                           
                                           for (int i  = 0; i < k; i++){
                                               check_offset = check_offset + col_entry[i].col_len +  1;
                                           }
                                           
                                           int extra = col_offset;
                                           int check_extra = check_offset;
                                           for ( int cur_record = 0; cur_record < temp_header->num_records; cur_record++ ){

                                               if ( col_entry[k].col_type == T_INT ){
                                                   int* size = (int*)malloc(1);
                                                   memcpy(size, (int*)((char*)header_and_contents_from_file+sizeof(table_file_header)+check_extra), 1);
                                                   check_extra++;
                                                   int* data = (int*)malloc(sizeof(int));
                                                   memcpy(data, (int*)((char*)header_and_contents_from_file+sizeof(table_file_header)+check_extra), *size);
                                                   
                                                   if (isnull){
                                                       if (*size==0){
                                                           if (isnot){
                                                               extra += temp_header->record_size;
                                                               check_extra = check_extra + temp_header->record_size - 1;
                                                               continue;
                                                           }
                                                           
                                                       }
                                                       else {
                                                           if (isnot){
                                                               
                                                           }else{
                                                               extra += temp_header->record_size;
                                                               check_extra = check_extra + temp_header->record_size - 1;
                                                               continue;
                                                           }
                                                       }
                                                   }
                                                   
                                                   else if (isnotnull){
                                                       if (*size!=0){
                                                           if (isnot){
                                                               extra += temp_header->record_size;
                                                               check_extra = check_extra + temp_header->record_size - 1;
                                                               continue;
                                                           }
                                                       }
                                                       else {
                                                           if (isnot){
                                                               
                                                           }else{
                                                           extra += temp_header->record_size;
                                                           check_extra = check_extra + temp_header->record_size - 1;
                                                           continue;
                                                           }
                                                       }
                                                       
                                                   }
                                                   
                                                   else if ( rel == S_EQUAL ){
                                                       
                                                       if (cur->tok_value == K_NULL && *size == 0){
                                                           if (isnot){
                                                               extra += temp_header->record_size;
                                                               check_extra = check_extra + temp_header->record_size - 1;
                                                               continue;
                                                           }
                                                       }
                                                       else if (*data == atoi(cur->tok_string) && cur->tok_value != K_NULL && *size != 0){
                                                           if (isnot){
                                                               extra += temp_header->record_size;
                                                               check_extra = check_extra + temp_header->record_size - 1;
                                                               continue;
                                                           }
                                                       }
                                                       else {
                                                           if (isnot){
                                                               
                                                           }
                                                           else{
                                                           extra += temp_header->record_size;
                                                           check_extra = check_extra + temp_header->record_size - 1;
                                                           continue;
                                                           }
                                                       }
                                                   }
                                                   
                                                   else if ( rel == S_GREATER ){
                                                       
                                                       if (*data > atoi(cur->tok_string) && cur->tok_value != K_NULL && *size != 0){
                                                           if (isnot){
                                                               extra += temp_header->record_size;
                                                               check_extra = check_extra + temp_header->record_size - 1;
                                                               continue;
                                                           }
                                                           
                                                       }else {
                                                           if (isnot){
                                                               
                                                           }
                                                           else {
                                                           extra += temp_header->record_size;
                                                           check_extra = check_extra + temp_header->record_size - 1;
                                                           continue;
                                                           }
                                                       }
                                                   }
                                                   
                                                   else if (rel == S_LESS){
                                                       
                                                       if (*data < atoi(cur->tok_string) && cur->tok_value != K_NULL && *size != 0){

                                                           if (isnot){

                                                               extra += temp_header->record_size;
                                                               check_extra = check_extra + temp_header->record_size - 1;
                                                               continue;
                                                           }
                                                           
                                                       }else {
                                                           if (isnot){
                                                               
                                                           }
                                                           else {
                                                           extra += temp_header->record_size;
                                                           check_extra = check_extra + temp_header->record_size - 1;
                                                           continue;
                                                           }
                                                       }
                                                   }
                                                   
                                                   if (is_int == 0){
                                                       memset((char*)header_and_contents_from_file + sizeof(table_file_header) + extra, 4, 1);
                                                       extra++;
                                                       memset((char*)header_and_contents_from_file + sizeof(table_file_header) + extra, '\0', sizeof(int));
                                                       memcpy(((char*)header_and_contents_from_file + sizeof(table_file_header) +extra),&new_value, sizeof(int));
                                                       extra--;
                                                       records_updated++;

                                                   }
                                                   
                                                   else if (is_int == 1){
                                                       memset((char*)header_and_contents_from_file + sizeof(table_file_header) + extra,strlen(str), 1);
                                                       extra++;
                                                       memset((char*)header_and_contents_from_file + sizeof(table_file_header) + extra, '\0', col_entry[j].col_len);
                                                       memcpy(((char*)header_and_contents_from_file + sizeof(table_file_header) +extra),str, col_entry[j].col_len);
                                                       extra--;
                                                       records_updated++;
                                                   }
                                                   
                                                   else if (is_int == -1){
                                                       memset((char*)header_and_contents_from_file + sizeof(table_file_header) + extra, 0, 1);
                                                       extra++;
                                                       memset((char*)header_and_contents_from_file + sizeof(table_file_header) + extra, '\0', col_entry[j].col_len);
                                                       extra--;
                                                       records_updated++;
                                                   }
                                               }
                                               else {
                                                   
                                                   int* size = (int*)malloc(1);
                                                   memcpy(size, (int*)((char*)header_and_contents_from_file+sizeof(table_file_header)+check_extra), 1);
                                                   check_extra++;
                                                   char data[*size];
                                                   memcpy(data, (char*)((char*)header_and_contents_from_file+sizeof(table_file_header)+check_extra), *size);
                                                   data[*size] = '\0';
                                                   
                                                   if (isnull){
                                                       if (*size==0){
                                                           if (isnot){
                                                               extra += temp_header->record_size;
                                                               check_extra = check_extra + temp_header->record_size - 1;
                                                               continue;
                                                           }
                                                       }
                                                       else {
                                                           if (isnot){
                                                               
                                                           }
                                                           else{

                                                           extra += temp_header->record_size;
                                                           check_extra = check_extra + temp_header->record_size - 1;
                                                           continue;
                                                           }
                                                       }
                                                   }
                                                   
                                                   else if (isnotnull){
                                                       if (*size!=0){
                                                           if (isnot){
                                                               extra += temp_header->record_size;
                                                               check_extra = check_extra + temp_header->record_size - 1;
                                                               continue;
                                                           }
                                                       }
                                                       else {
                                                           if (isnot){
                                                               
                                                           }
                                                           else{
                                                           extra += temp_header->record_size;
                                                           check_extra = check_extra + temp_header->record_size - 1;
                                                           continue;
                                                           }
                                                       }
                                                       
                                                   }
                                                   else if ( rel == S_EQUAL ){
                                                       
                                                       if (cur->tok_value == K_NULL && *size == 0){
                                                           if (isnot){
                                                               extra += temp_header->record_size;
                                                               check_extra = check_extra + temp_header->record_size - 1;
                                                               continue;
                                                           }
                                                       }
                                                       else if (strcmp(data,cur->tok_string)==0 && cur->tok_value != K_NULL && *size != 0){
                                                           if (isnot){
                                                               extra += temp_header->record_size;
                                                               check_extra = check_extra + temp_header->record_size - 1;
                                                               continue;
                                                           }
                                                       }
                                                       else {
                                                           if (isnot){
                                                               
                                                           }
                                                           else{
                                                           extra += temp_header->record_size;
                                                           check_extra = check_extra + temp_header->record_size - 1;
                                                           continue;
                                                           }
                                                       }
                                                   }
                                                   else if ( rel == S_GREATER ){
                                                       
                                                      if (strcmp(data,cur->tok_string)>0 && cur->tok_value != K_NULL && *size != 0){
                                                           if (isnot){
                                                               extra += temp_header->record_size;
                                                               check_extra = check_extra + temp_header->record_size - 1;
                                                               continue;
                                                           }
                                                       }
                                                       else {
                                                           if (isnot){
                                                               
                                                           }
                                                           else{
                                                           extra += temp_header->record_size;
                                                           check_extra = check_extra + temp_header->record_size - 1;
                                                           continue;
                                                           }
                                                       }
                                                   }
                                                   else if ( rel == S_LESS ){
                                                       
                                                       if (strcmp(data,cur->tok_string)<0 && cur->tok_value != K_NULL && *size != 0){
                                                           if (isnot){
                                                               extra += temp_header->record_size;
                                                               check_extra = check_extra + temp_header->record_size - 1;
                                                               continue;
                                                           }
                                                       }
                                                       else {
                                                           if (isnot){
                                                               
                                                           }
                                                           else{
                                                           extra += temp_header->record_size;
                                                           check_extra = check_extra + temp_header->record_size - 1;
                                                           continue;
                                                           }
                                                       }
                                                   }
                                                
                                            
                                                   if (is_int == 0){
                                                       memset((char*)header_and_contents_from_file + sizeof(table_file_header) + extra, 4, 1);
                                                       extra++;
                                                       memset((char*)header_and_contents_from_file + sizeof(table_file_header) + extra, '\0', sizeof(int));
                                                       memcpy(((char*)header_and_contents_from_file + sizeof(table_file_header) +extra),&new_value, sizeof(int));
                                                       extra--;
                                                       records_updated++;

                                                   }
                                                   else if (is_int == 1){
                                                       memset((char*)header_and_contents_from_file + sizeof(table_file_header) + extra,strlen(str), 1);
                                                       extra++;
                                                       memset((char*)header_and_contents_from_file + sizeof(table_file_header) + extra, '\0', col_entry[j].col_len);
                                                       memcpy(((char*)header_and_contents_from_file + sizeof(table_file_header) +extra),str, col_entry[j].col_len);
                                                       extra--;
                                                       records_updated++;
                                                   }
                                                   else if (is_int == -1){
                                                       memset((char*)header_and_contents_from_file + sizeof(table_file_header) + extra, 0, 1);
                                                       extra++;
                                                       memset((char*)header_and_contents_from_file + sizeof(table_file_header) + extra, '\0', col_entry[j].col_len);
                                                       extra--;
                                                       records_updated++;
                                                   }
                                               }
                                               extra += temp_header->record_size;
                                               check_extra = check_extra + temp_header->record_size - 1;
                                           }
                                   }
                                   }
                               }
                           }
                           else {
                               int extra = col_offset;
                               for ( int cur_record = 0; cur_record < temp_header->num_records; cur_record++ ){
                                   if (is_int == 0){
                                       memset((char*)header_and_contents_from_file + sizeof(table_file_header) + extra, 4, 1);
                                       extra++;
                                       memset((char*)header_and_contents_from_file + sizeof(table_file_header) + extra, '\0', sizeof(int));
                                       memcpy(((char*)header_and_contents_from_file + sizeof(table_file_header) +extra),&new_value, sizeof(int));
                                       records_updated++;

                                   }
                                   
                                   else if (is_int == 1){
                                       memset((char*)header_and_contents_from_file + sizeof(table_file_header) + extra,strlen(str), 1);
                                       extra++;
                                       memset((char*)header_and_contents_from_file + sizeof(table_file_header) + extra, '\0', col_entry[j].col_len);
                                       memcpy(((char*)header_and_contents_from_file + sizeof(table_file_header) +extra),str, col_entry[j].col_len);
                                       records_updated++;
                                                                              
                                   }
                                   
                                   else if (is_int == -1){
                                       memset((char*)header_and_contents_from_file + sizeof(table_file_header) + extra, 0, 1);
                                       extra++;
                                       memset((char*)header_and_contents_from_file + sizeof(table_file_header) + extra, '\0', col_entry[j].col_len);
                                       records_updated++;
                                   }
                                   extra = extra + temp_header->record_size - 1;
                               }
                           }
                            if(records_updated == 0)
                            {
                                printf("No record found\n");
                            }
                            else if(records_updated == 1)
                            {
                                printf("1 record updated\n");
                            }
                             else
                            {
                                printf("%d records updated\n",records_updated);
                                
                            }
                        }
                        fhandle = fopen(tab_name, "wbc");
                        fwrite(header_and_contents_from_file, ((sizeof(table_file_header)+header_and_contents_from_file->record_size * header_and_contents_from_file->num_records)), 1, fhandle);
                        fflush(fhandle);
                        fclose(fhandle);
                    }
                }
            }
        }
    return rc;
}

// final

int calculate_count(int position, char filename[], std::pair<int,char[]>* records, cd_entry  *col_entry){
    int count = 0;
    FILE *fhandle = NULL;
    fhandle = fopen(filename, "rbc");
    table_file_header* temp_header = NULL;
    temp_header = (table_file_header*)calloc(1, sizeof(table_file_header));
    fread(temp_header,sizeof(table_file_header), 1, fhandle);
    fflush(fhandle);
    fclose(fhandle);
    
    fhandle = fopen(filename, "rbc");
    table_file_header* header_and_contents_from_file = NULL;
    header_and_contents_from_file = (table_file_header*)malloc((100 * temp_header->record_size)+ sizeof(table_file_header));
    fread(header_and_contents_from_file,temp_header->file_size, 1, fhandle);
    fflush(fhandle);
    fclose(fhandle);
    
    // -1 is *
    if ( position == -1) {
        for ( int cur_record = 0; cur_record < temp_header->num_records; cur_record++ ){
            
            if (records[cur_record].first == 1){
                count++;
            }
        }
        return count;
        
    }
    
    
    int col_offset = 0;
    for (int i  = 0; i < position; i++){
        col_offset = col_offset + col_entry[i].col_len +  1;
    }

    int extra = col_offset;


    for ( int cur_record = 0; cur_record < temp_header->num_records; cur_record++ ){
        
        if (records[cur_record].first == 0){
            extra+=header_and_contents_from_file->record_size;
            continue;
        }
        else {
            int* size = (int*)malloc(1);
            memcpy(size, (int*)((char*)header_and_contents_from_file+sizeof(table_file_header)+extra), 1);
            extra++;
            if (*size != 0){
                count++;
            }
            extra--;
            extra+=header_and_contents_from_file->record_size;
        }
    }
    return count;
    

}

void print_count(int position, char filename[], std::pair<int,char[]>* records, cd_entry  *col_entry){
    int count = calculate_count( position, filename, records, col_entry);
    if ( position == -1){
        printf("      COUNT(*)      ");;
    }
    else{
        printf("      COUNT(%s)      ",col_entry[position].col_name);
    }
    printf("\n --------------------------------------------------------------------------------------------------------- \n");
    printf("%10d       \n",count);

}


int calculate_sum(int position, char filename[], std::pair<int,char[]>* records, cd_entry  *col_entry){
    
    int sum = 0;
    FILE *fhandle = NULL;
    fhandle = fopen(filename, "rbc");
    table_file_header* temp_header = NULL;
    temp_header = (table_file_header*)calloc(1, sizeof(table_file_header));
    fread(temp_header,sizeof(table_file_header), 1, fhandle);
    fflush(fhandle);
    fclose(fhandle);
    
    fhandle = fopen(filename, "rbc");
    table_file_header* header_and_contents_from_file = NULL;
    header_and_contents_from_file = (table_file_header*)malloc((100 * temp_header->record_size)+ sizeof(table_file_header));
    fread(header_and_contents_from_file,temp_header->file_size, 1, fhandle);
    fflush(fhandle);
    fclose(fhandle);
    
    
    
    int col_offset = 0;
    for (int i  = 0; i < position; i++){
        col_offset = col_offset + col_entry[i].col_len +  1;
    }

    int extra = col_offset;


    for ( int cur_record = 0; cur_record < temp_header->num_records; cur_record++ ){
        
        if (records[cur_record].first == 0){
            extra+=header_and_contents_from_file->record_size;
            continue;
        }
        else {
            int* data = (int*)malloc(sizeof(int));
            int* size = (int*)malloc(1);
            memcpy(size, (int*)((char*)header_and_contents_from_file+sizeof(table_file_header)+extra), 1);
            extra++;
            if (*size != 0){

                memcpy(data, (int*)((char*)header_and_contents_from_file+sizeof(table_file_header)+extra), *size);
                sum+=*data;
            
            }
            extra--;
            extra+=header_and_contents_from_file->record_size;
        }
    }
    return sum;

}


void print_sum(int position, char filename[], std::pair<int,char[]>* records, cd_entry  *col_entry){
    int sum = calculate_sum( position, filename, records, col_entry);
    printf("      SUM(%s)      ",col_entry[position].col_name);
    printf("\n --------------------------------------------------------------------------------------------------------- \n");
    printf("%10d       \n",sum);
}


float calculate_avg(int position, char filename[], std::pair<int,char[]>* records, cd_entry  *col_entry){
    
    return (float)calculate_sum(position, filename, records, col_entry)/(float)calculate_count(position, filename, records, col_entry);

}

void print_avg(int position, char filename[], std::pair<int,char[]>* records, cd_entry  *col_entry){

    float avg = calculate_avg( position, filename, records, col_entry);
    printf("      AVG(%s)      ",col_entry[position].col_name);
    printf("\n --------------------------------------------------------------------------------------------------------- \n");
    printf("%10f       \n",avg);
}

bool check_if_in_char_array(char data_original[255][255], char to_find[], int records){
    for ( int cur_record = 0; cur_record < records; cur_record++ ){
        if(strcmp(to_find, data_original[cur_record])==0)
        {
            return true;
        }
    }
    return false;
}

bool check_if_in_int_array(int data_original[], int to_find, int records){
    for ( int cur_record = 0; cur_record < records; cur_record++ ){
        if(data_original[cur_record] == to_find)
        {
            return true;
        }
    }
    return false;
}

void get_records_that_match_char(char data_original[255], int size_original, std::pair<int,char[]>* records, char filename[],  cd_entry  *col_entry, int col_search_index, std::pair<int,char[]>* records_answer ){
        
    FILE *fhandle = NULL;
    fhandle = fopen(filename, "rbc");
    table_file_header* temp_header = NULL;
    temp_header = (table_file_header*)calloc(1, sizeof(table_file_header));
    fread(temp_header,sizeof(table_file_header), 1, fhandle);
    fflush(fhandle);
    fclose(fhandle);
    
    fhandle = fopen(filename, "rbc");
    table_file_header* header_and_contents_from_file = NULL;
    header_and_contents_from_file = (table_file_header*)malloc((100 * temp_header->record_size)+ sizeof(table_file_header));
    fread(header_and_contents_from_file,temp_header->file_size, 1, fhandle);
    fflush(fhandle);
    fclose(fhandle);
    
    int extra = 0;

    
    for ( int cur_record = 0; cur_record < temp_header->num_records; cur_record++ ){
        
        int tmp_cur_record = cur_record;
        extra = cur_record*header_and_contents_from_file->record_size;

        if (records[tmp_cur_record].first == 0){
            continue;
        }
        else {
            int j = col_search_index;
                    
                    int col_offset = 0;
                    for (int i  = 0; i < j; i++){
                        col_offset = col_offset + col_entry[i].col_len +  1;
                    }
                    
                    int col_extra = col_offset + extra;
                    
                    if (col_entry[j].col_type == T_CHAR){
                        int* size = (int*)malloc(1);
                        memcpy(size, (int*)((char*)header_and_contents_from_file+sizeof(table_file_header)+col_extra), 1);
                        col_extra++;
                        if (*size != 0 && size_original != 0){
                            char data[*size];
                            memcpy(data, (char*)((char*)header_and_contents_from_file+sizeof(table_file_header)+col_extra), *size);
                            data[*size] = '\0';
                            if (strcmp(data,data_original)==0){
                                records_answer[cur_record].first=1;
                            }
                        }
                        else{
                            if (size_original==0 && *size == 0){
                                records_answer[cur_record].first=1;
                            }
                        }
                    }
                }
    }
}

void get_records_that_match_int(int data_original, int size_original, std::pair<int,char[]>* records, char filename[],  cd_entry  *col_entry, int col_search_index, std::pair<int,char[]>* records_answer ){
        
    FILE *fhandle = NULL;
    fhandle = fopen(filename, "rbc");
    table_file_header* temp_header = NULL;
    temp_header = (table_file_header*)calloc(1, sizeof(table_file_header));
    fread(temp_header,sizeof(table_file_header), 1, fhandle);
    fflush(fhandle);
    fclose(fhandle);
    
    fhandle = fopen(filename, "rbc");
    table_file_header* header_and_contents_from_file = NULL;
    header_and_contents_from_file = (table_file_header*)malloc((100 * temp_header->record_size)+ sizeof(table_file_header));
    fread(header_and_contents_from_file,temp_header->file_size, 1, fhandle);
    fflush(fhandle);
    fclose(fhandle);
    
    int extra = 0;

    
    for ( int cur_record = 0; cur_record < temp_header->num_records; cur_record++ ){
        
        int tmp_cur_record = cur_record;
        extra = cur_record*header_and_contents_from_file->record_size;

        if (records[tmp_cur_record].first == 0){
            continue;
        }
        else {
            int j = col_search_index;
                    
                    int col_offset = 0;
                    for (int i  = 0; i < j; i++){
                        col_offset = col_offset + col_entry[i].col_len +  1;
                    }
                    
                    int col_extra = col_offset + extra;
                    
                    if (col_entry[j].col_type == T_CHAR){
                    }
                    else{
                        
                        int* size = (int*)malloc(1);
                        memcpy(size, (int*)((char*)header_and_contents_from_file+sizeof(table_file_header)+col_extra), 1);
                        col_extra++;
                        int* data = (int*)malloc(sizeof(int));
                        if (*size != 0 && size_original != 0){
                            memcpy(data, (int*)((char*)header_and_contents_from_file+sizeof(table_file_header)+col_extra), *size);
                            if (*data == data_original){
                                records_answer[cur_record].first=1;
                            }
                        }
                        else{
                            if (size_original==0 && *size == 0){
                                records_answer[cur_record].first=1;
                            }
                        }
                        
                    }
                }
    }
}

//pranavi
void print_group_by(int index[], char filename[],std::pair<int,char[]>* records, cd_entry  *col_entry, int num_col, int order[], int group_agg, int agg_col_index, int group_col_index){
    

    int records_printed = 0;
    FILE *fhandle = NULL;
    fhandle = fopen(filename, "rbc");
    table_file_header* temp_header = NULL;
    temp_header = (table_file_header*)calloc(1, sizeof(table_file_header));
    fread(temp_header,sizeof(table_file_header), 1, fhandle);
    fflush(fhandle);
    fclose(fhandle);
    
    fhandle = fopen(filename, "rbc");
    table_file_header* header_and_contents_from_file = NULL;
    header_and_contents_from_file = (table_file_header*)malloc((100 * temp_header->record_size)+ sizeof(table_file_header));
    fread(header_and_contents_from_file,temp_header->file_size, 1, fhandle);
    fflush(fhandle);
    fclose(fhandle);
    
    int skip_new_line = 0;
    
    int char_found = 0;
    char char_array[255][255];
    
    int int_found = 0;
    int int_array[temp_header->num_records];

    
    
    
    for (int j=0;j<num_col;j++)
    { printf("      %s      ",col_entry[index[j]].col_name);}
    
    if ( group_agg == F_SUM){
        printf("      SUM(%s)      ",col_entry[agg_col_index].col_name);
    }
    if ( group_agg == F_AVG){
        printf("      AVG(%s)      ",col_entry[agg_col_index].col_name);
    }
    if ( group_agg == F_COUNT){
        printf("      COUNT(%s)      ",col_entry[agg_col_index].col_name);
    }
    printf("\n --------------------------------------------------------------------------------------------------------- \n");
    int extra = 0;
    

    for ( int cur_record = 0; cur_record < temp_header->num_records; cur_record++ ){
        
        std::pair<int,char[]> records_to_agg[temp_header->num_records];

        int tmp_cur_record = cur_record;
        if (order != NULL){
            tmp_cur_record = order[cur_record];
            extra = tmp_cur_record*header_and_contents_from_file->record_size;
        }
        else {
            extra = cur_record*header_and_contents_from_file->record_size;
        }

        if (records[tmp_cur_record].first == 0){
            continue;
        }
        
        else {
            records_printed++;
                for (int j=0;j<num_col;j++)
                {

                    
                    int col_offset = 0;
                    for (int i  = 0; i < index[j]; i++){
                        col_offset = col_offset + col_entry[i].col_len +  1;
                    }
                    
                    int col_extra = col_offset + extra;
                    if (col_entry[index[j]].col_type == T_CHAR){
                        int* size = (int*)malloc(1);
                        memcpy(size, (int*)((char*)header_and_contents_from_file+sizeof(table_file_header)+col_extra), 1);
                        col_extra++;
                        char data[*size];
                        if (*size != 0){
                            memcpy(data, (char*)((char*)header_and_contents_from_file+sizeof(table_file_header)+col_extra), *size);
                            data[*size] = '\0';
                            if (check_if_in_char_array(char_array, data, char_found)){
                                skip_new_line = 1;
                                continue;
                            }
                            else {
                                skip_new_line = 0;
                                strcpy(char_array[char_found],data);
                                char_found++;
                                
                                
                            }
                                printf("       %-12s",data);
                        }
                        else{
                            strcpy(data,"null");
                            if (check_if_in_char_array(char_array, data, char_found)){
                                skip_new_line = 1;
                                continue;
                            }
                            else {
                                skip_new_line = 0;
                                strcpy(char_array[char_found],data);
                                char_found++;
                                
                                
                            }
                            printf("       -           ");
                        }
                        if ( index[j] == group_col_index ){
                                                        
                            get_records_that_match_char(data,*size,records,filename,col_entry,group_col_index,records_to_agg);
                            if (group_agg == F_SUM){
                                int agg = calculate_sum(agg_col_index, filename, records_to_agg, col_entry);
                                printf("%10d       ",agg);
                            }
                            if (group_agg == F_COUNT){
                                int agg = calculate_count(agg_col_index, filename, records_to_agg, col_entry);
                                printf("%10d       ",agg);
                            }
                            if (group_agg == F_AVG){
                                float agg = calculate_avg(agg_col_index, filename, records_to_agg, col_entry);
                                printf("%10f       ",agg);
                            }
                        }
                    }
                    else{
                        int* data = (int*)malloc(sizeof(int));
                        int* size = (int*)malloc(1);
                        memcpy(size, (int*)((char*)header_and_contents_from_file+sizeof(table_file_header)+col_extra), 1);
                        col_extra++;
                        if (*size != 0){

                      memcpy(data, (int*)((char*)header_and_contents_from_file+sizeof(table_file_header)+col_extra), *size);
                            if (check_if_in_int_array(int_array, *data, int_found)){
                                skip_new_line = 1;
                                continue;
                            }
                            else {
                                skip_new_line = 0;
                                int_array[int_found] = *data;
                                int_found++;
                            }
                            printf("%10d       ",*data);

                        }
                        else{
                            *data = 100000000;
                            if (check_if_in_int_array(int_array, *data, int_found)){
                                skip_new_line = 1;
                                continue;
                            }
                            else {
                                skip_new_line = 0;
                                int_array[int_found] = *data;
                                int_found++;
                            }
                            printf("        -        ");
                        }
                        if ( index[j] == group_col_index ){
                                                        
                            get_records_that_match_int(*data,*size,records,filename,col_entry,group_col_index,records_to_agg);
                            if (group_agg == F_SUM){
                                int agg = calculate_sum(agg_col_index, filename, records_to_agg, col_entry);
                                printf("%10d       ",agg);
                            }
                            if (group_agg == F_COUNT){
                                int agg = calculate_count(agg_col_index, filename, records_to_agg, col_entry);
                                printf("%10d       ",agg);
                            }
                            if (group_agg == F_AVG){
                                float agg = calculate_avg(agg_col_index, filename, records_to_agg, col_entry);
                                printf("%10f       ",agg);
                            }
                        }
                    }
                }
            if ( skip_new_line == 0){
                    printf("\n");
            }
                }
    }
    
    
    
    
}

void print_rows(int index[], char filename[], std::pair<int,char[]>* records, cd_entry  *col_entry, int num_col, int order[] = NULL){
    
    int records_printed = 0;
    FILE *fhandle = NULL;
    fhandle = fopen(filename, "rbc");
    table_file_header* temp_header = NULL;
    temp_header = (table_file_header*)calloc(1, sizeof(table_file_header));
    fread(temp_header,sizeof(table_file_header), 1, fhandle);
    fflush(fhandle);
    fclose(fhandle);
    
    fhandle = fopen(filename, "rbc");
    table_file_header* header_and_contents_from_file = NULL;
    header_and_contents_from_file = (table_file_header*)malloc((100 * temp_header->record_size)+ sizeof(table_file_header));
    fread(header_and_contents_from_file,temp_header->file_size, 1, fhandle);
    fflush(fhandle);
    fclose(fhandle);
    
    for (int j=0;j<num_col;j++)
    { printf("      %s      ",col_entry[index[j]].col_name);}
    
    printf("\n --------------------------------------------------------------------------------------------------------- \n");
    int extra = 0;
    
    
    for ( int cur_record = 0; cur_record < temp_header->num_records; cur_record++ ){
        
        int tmp_cur_record = cur_record;
        if (order != NULL){
            tmp_cur_record = order[cur_record];
            extra = tmp_cur_record*header_and_contents_from_file->record_size;
        }
        else {
            extra = cur_record*header_and_contents_from_file->record_size;
        }

        if (records[tmp_cur_record].first == 0){
            continue;
        }
        else {
            records_printed++;
                for (int j=0;j<num_col;j++)
                {
                    int col_offset = 0;
                    for (int i  = 0; i < index[j]; i++){
                        col_offset = col_offset + col_entry[i].col_len +  1;
                    }
                    
                    int col_extra = col_offset + extra;
                    
                    if (col_entry[index[j]].col_type == T_CHAR){
                        int* size = (int*)malloc(1);
                        memcpy(size, (int*)((char*)header_and_contents_from_file+sizeof(table_file_header)+col_extra), 1);
                        col_extra++;
                        if (*size != 0){
                            char data[*size];
                            memcpy(data, (char*)((char*)header_and_contents_from_file+sizeof(table_file_header)+col_extra), *size);
                            data[*size] = '\0';
                            printf("       %-12s",data);
                        }
                        else{
                            printf("       -           ");
                        }
                    }
                    else{
                        int* data = (int*)malloc(sizeof(int));
                        int* size = (int*)malloc(1);
                        memcpy(size, (int*)((char*)header_and_contents_from_file+sizeof(table_file_header)+col_extra), 1);
                        col_extra++;
                        if (*size != 0){

                      memcpy(data, (int*)((char*)header_and_contents_from_file+sizeof(table_file_header)+col_extra), *size);
                            printf("%10d       ",*data);

                        }
                        else{
                            printf("        -        ");
                        }
                    }
                }
                    printf("\n");
                }
    }
    if(records_printed == 0)
    {
        printf("0 records selected\n");
    }
    else if(records_printed == 1)
    {
        printf("1 record selected\n");
    }
     else
    {
        printf("%d records selected\n",records_printed);
        
    }
}

void get_list(std::pair<int,char[]>* records, char filename[], cd_entry  *col_entry, int col_index, int rel, char tok_string[], int is_int){
    
    
    FILE *fhandle = NULL;
    fhandle = fopen(filename, "rbc");
    table_file_header* temp_header = NULL;
    temp_header = (table_file_header*)calloc(1, sizeof(table_file_header));
    fread(temp_header,sizeof(table_file_header), 1, fhandle);
    fflush(fhandle);
    fclose(fhandle);
    
    fhandle = fopen(filename, "rbc");
    table_file_header* header_and_contents_from_file = NULL;
    header_and_contents_from_file = (table_file_header*)malloc((100 * temp_header->record_size)+ sizeof(table_file_header));
    fread(header_and_contents_from_file,temp_header->file_size, 1, fhandle);
    fflush(fhandle);
    fclose(fhandle);
        
    int col_offset = 0;
    for (int i  = 0; i < col_index; i++){
        col_offset = col_offset + col_entry[i].col_len +  1;
    }
    
    int extra = col_offset;
    
    for ( int cur_record = 0; cur_record < temp_header->num_records; cur_record++ ){
        
        if ( col_entry[col_index].col_type == T_INT ){
                        
            int* size = (int*)malloc(1);
            memcpy(size, (int*)((char*)header_and_contents_from_file+sizeof(table_file_header)+extra), 1);
            extra++;
            int* data = (int*)malloc(sizeof(int));
            memcpy(data, (int*)((char*)header_and_contents_from_file+sizeof(table_file_header)+extra), *size);
            extra--;
            
            if ( rel == S_EQUAL ){
                
                if (is_int==-1 &&  *size == 0){

                }
                else if (*data == atoi(tok_string) && is_int!= -1 && *size != 0){
                    
                }
                else {
                    extra += temp_header->record_size;
                    records[cur_record].first = 0;
                    continue;
                }
            }
            
            else if ( rel == K_IS ){
                
                if (is_int==-1 &&  *size == 0){

                }
                else {
                    extra += temp_header->record_size;
                    records[cur_record].first = 0;
                    continue;
                }
                
            }
            
            else if ( rel == K_NOT ){
                
                if (is_int==-1 &&  *size != 0)
                {
                    //
                }
                else {
                    extra += temp_header->record_size;
                    records[cur_record].first = 0;
                    continue;
                }
                
            }
            
            else if ( rel == S_GREATER ){
                
                if (*data > atoi(tok_string) && is_int!=-1 && *size != 0){
                    
                }else {
                    extra += temp_header->record_size;
                    records[cur_record].first = 0;
                    continue;
                }
            }
            
            else if (rel == S_LESS){
                
                if (*data < atoi(tok_string) && is_int!=-1 && *size != 0){
                    
                }else {
                    extra += temp_header->record_size;
                    records[cur_record].first = 0;
                    continue;
                }
            }
            
            records[cur_record].first = 1;
        }
        else {
            int* size = (int*)malloc(1);
            memcpy(size, (int*)((char*)header_and_contents_from_file+sizeof(table_file_header)+extra), 1);
            extra++;
            char data[*size];
            memcpy(data, (char*)((char*)header_and_contents_from_file+sizeof(table_file_header)+extra), *size);
            data[*size] = '\0';
            extra--;
            
            if ( rel == S_EQUAL ){
                
                if (is_int==-1 && *size == 0){

                }
                else if (strcmp(data,tok_string)==0 && is_int!= -1 && *size != 0){
                    
                }
                else {
                    extra += temp_header->record_size;
                    records[cur_record].first = 0;
                    continue;
                }
            }
            
            else if ( rel == K_IS ){
                
                if (is_int==-1 &&  *size == 0){

                }
                else {
                    extra += temp_header->record_size;
                    records[cur_record].first = 0;
                    continue;
                }
                
            }
            
            else if ( rel == K_NOT ){
                
                if (is_int==-1 &&  *size != 0){

                }
                else {
                    extra += temp_header->record_size;
                    records[cur_record].first = 0;
                    continue;
                }
                
            }
            
            else if ( rel == S_GREATER ){
                
                if (strcmp(data,tok_string)>0 && is_int!=-1 && *size != 0){
                    
                }else {
                    extra += temp_header->record_size;
                    records[cur_record].first = 0;
                    continue;
                }
            }
            
            else if (rel == S_LESS){
                
                if (strcmp(data,tok_string)<0 && *size != 0){
                    
                }else {
                    extra += temp_header->record_size;
                    records[cur_record].first = 0;
                    continue;
                }
            }
            records[cur_record].first = 1;
        }
        extra += temp_header->record_size;
    }
}


void merge_records_and(std::pair<int,char[]>* base_records, std::pair<int,char[]>* new_records, int num_records){
    
    for ( int i = 0; i < num_records; i++){
        base_records[i].first = base_records[i].first && new_records[i].first;
    }
}

void merge_records_or(std::pair<int,char[]>* base_records, std::pair<int,char[]>* new_records, int num_records){
    
    for ( int i = 0; i < num_records; i++){
        base_records[i].first = base_records[i].first || new_records[i].first;
    }
}

void negate_records(std::pair<int,char[]>* base_records, int num_records){
    for ( int i = 0; i < num_records; i++){
        base_records[i].first = !(base_records[i].first);
    }
}

int get_records(char filename[]){
    
    FILE *fhandle = NULL;
    fhandle = fopen(filename, "rbc");
    table_file_header* temp_header = NULL;
    temp_header = (table_file_header*)calloc(1, sizeof(table_file_header));
    fread(temp_header,sizeof(table_file_header), 1, fhandle);
    fflush(fhandle);
    fclose(fhandle);
    
    return temp_header->record_size;
    
}

typedef struct {
    int first;
    int second;
} int_pair;

typedef struct {
    int first;
    char second[255];
} str_pair;

int asc_str_compare( const void* a, const void* b)
{
     char* int_a = ((str_pair*)a)->second;
     char* int_b = ((str_pair*)b)->second;

    return strcmp(int_a,int_b);
}

int desc_str_compare( const void* a, const void* b)
{
     char* int_a = ((str_pair*)a)->second;
     char* int_b = ((str_pair*)b)->second;

    return -strcmp(int_a,int_b);
}

int asc_int_compare( const void* a, const void* b)
{
     int int_a = ((int_pair*)a)->second;
     int int_b = ((int_pair*)b)->second;

     if ( int_a == int_b ) return 0;
     else if ( int_a < int_b ) return -1;
     else return 1;
}

int desc_int_compare( const void* a, const void* b)
{
     int int_a = ((int_pair*)a)->second;
     int int_b = ((int_pair*)b)->second;

     if ( int_a == int_b ) return 0;
     else if ( int_a > int_b ) return -1;
     else return 1;
}

void get_record_order(std::pair<int,char[]>* records, char filename[], cd_entry  *col_entry, int col_index, int order[],bool is_asc){
    

    FILE *fhandle = NULL;
    fhandle = fopen(filename, "rbc");
    table_file_header* temp_header = NULL;
    temp_header = (table_file_header*)calloc(1, sizeof(table_file_header));
    fread(temp_header,sizeof(table_file_header), 1, fhandle);
    fflush(fhandle);
    fclose(fhandle);

    fhandle = fopen(filename, "rbc");
    table_file_header* header_and_contents_from_file = NULL;
    header_and_contents_from_file = (table_file_header*)malloc((100 * temp_header->record_size)+ sizeof(table_file_header));
    fread(header_and_contents_from_file,temp_header->file_size, 1, fhandle);
    fflush(fhandle);
    fclose(fhandle);

    int col_offset = 0;
    for (int i  = 0; i < col_index; i++){
        col_offset = col_offset + col_entry[i].col_len +  1;
    }

    int extra = col_offset;
    
    str_pair new_records_string[temp_header->num_records];
    int_pair new_records_int[temp_header->num_records];
    bool is_int = true;

    for ( int cur_record = 0; cur_record < temp_header->num_records; cur_record++ ){

        if ( col_entry[col_index].col_type == T_INT ){
            
            
            is_int = true;
            int* size = (int*)malloc(1);
            memcpy(size, (int*)((char*)header_and_contents_from_file+sizeof(table_file_header)+extra), 1);
            extra++;
            int* data = (int*)malloc(sizeof(int));
            memcpy(data, (int*)((char*)header_and_contents_from_file+sizeof(table_file_header)+extra), *size);
            extra--;
            new_records_int[cur_record].first = cur_record;
            new_records_int[cur_record].second = *data;
        }
        else {
            is_int = false;
            int* size = (int*)malloc(1);
            memcpy(size, (int*)((char*)header_and_contents_from_file+sizeof(table_file_header)+extra), 1);
            extra++;
            char data[*size];
            memcpy(data, (char*)((char*)header_and_contents_from_file+sizeof(table_file_header)+extra), *size);
            data[*size] = '\0';
            extra--;
            new_records_string[cur_record].first = cur_record;
            strcpy(new_records_string[cur_record].second,data);
            
        }
        
        
        extra += temp_header->record_size;
    }
    
    if(is_int){
    
        if (is_asc){
            qsort(new_records_int, temp_header->num_records, sizeof(int_pair), asc_int_compare);
        }
        else {
            qsort(new_records_int, temp_header->num_records, sizeof(int_pair), desc_int_compare);
        }
    
        for ( int cur_record = 0; cur_record < temp_header->num_records; cur_record++ ){
            order[cur_record] = new_records_int[cur_record].first;
        }
    }
    else {
        
        if (is_asc){
            qsort(new_records_string, temp_header->num_records, sizeof(str_pair), asc_str_compare);
        }
        else {
            qsort(new_records_string, temp_header->num_records, sizeof(str_pair), desc_str_compare);
        }
    
        for ( int cur_record = 0; cur_record < temp_header->num_records; cur_record++ ){
            order[cur_record] = new_records_string[cur_record].first;
        }
        
        
    }
}


int sem_select(token_list *t_list)
{
    int rc=0,j,rel1,rel2,val1,val2,copy_size1 =0,copy_size2 =0;
    token_list* temp = NULL;
    token_list* temp1 = NULL;
    token_list* group_temp = NULL;
    token_list* check = NULL;
    token_list* check_group = NULL;
    char select_table[20][MAX_IDENT_LEN+4];
    char table_where_first[MAX_IDENT_LEN+4];
    char table_where_second[MAX_IDENT_LEN+4];
    char shared_columns[20][MAX_IDENT_LEN+4];
    int shared_index[20];
    int num_shared_columns =0;
    for (int i=0;i<20;i++)
       {
           strcpy(select_table[i],"o");
       }
    FILE *fhandle = NULL;
    
    int col_to_print = 0;
    std::pair<int,char[]> records[100];

    int order[100];

    token_list *cur;
    cur = t_list;
    char tab_name[MAX_IDENT_LEN+4];
    char tab_name1[MAX_IDENT_LEN+4];
    char tab_name2[MAX_IDENT_LEN+4];
    char tab_name3[MAX_IDENT_LEN+4];
    char file_name[MAX_IDENT_LEN+4];
    char check_column1[MAX_IDENT_LEN+4];
    char check_column2[MAX_IDENT_LEN+4];
    char check_column_orderby[MAX_IDENT_LEN+4];
    tpd_entry *new_entry = NULL;
    tpd_entry *temp_entry = NULL;
    cd_entry  *col_entry = NULL;
    tpd_entry *new_entry1 = NULL;
    tpd_entry *temp_entry1 = NULL;
    cd_entry  *col_entry1 = NULL;
    tpd_entry *temp_entry2 = NULL;
    cd_entry  *col_entry2 = NULL;
    tpd_entry *temp_entry3 = NULL;
    cd_entry  *col_entry3 = NULL;
    bool isnot1 = false;
    bool isnot2 = false;
    bool is_ordered = false;
    int group_col_index;
    bool group_col_found = false;
    bool group_count_is_star = false;
    char group_agg_col[MAX_IDENT_LEN+4];
    char group_col[MAX_IDENT_LEN+4];
    int group_agg_fn =-1;
    bool group = false;
    
    // -1 means no agg fn.
    int agg_fn = -1;
    char aggr_parameter[MAX_IDENT_LEN+4];
    bool count_is_star = false;
    bool found = false;
    bool found1 = false;
    bool isnull1 =false;
    bool isnotnull1 = false;
    bool isnull2 = false;
    bool isnotnull2 = false;
    bool isdesc=false;
    int index[20];
    int index_one;
    int index_two;
    int index_order;
    int index_group;
    int cond;
    bool group_found = false;
    char to_print[20][MAX_IDENT_LEN+4];
    int num=0;// AND = 1 , OR =2
    if(cur->tok_value!=F_SUM && cur->tok_value!=F_AVG && cur->tok_value!=F_COUNT && cur->tok_value!=IDENT && cur->tok_value!=S_STAR)
    {
        rc = INVALID_STATEMENT;
        cur->tok_value = INVALID;
        return rc;
    }
    else
    {

     if(cur->tok_value == F_SUM || cur->tok_value == F_AVG)
     {
         agg_fn = cur->tok_value;
         cur = cur->next;
         if(cur->tok_value!=S_LEFT_PAREN)
         {

             rc = INVALID_SYNTAX;
             cur->tok_value = INVALID;
             return rc;
         }
         else
         {
             cur = cur->next;
             if(cur->tok_value != IDENT)
             {
                 rc = INVALID_SYNTAX;
                 cur->tok_value = INVALID;
                 return rc;
             }
             else
             {
                 strcpy(aggr_parameter,cur->tok_string);
                 temp = cur;
             }
             cur = cur->next;
             if(cur->tok_value!=S_RIGHT_PAREN)
             {

                 rc = INVALID_SYNTAX;
                 cur->tok_value = INVALID;
                 return rc;
             }
             else
             {

                 cur = cur->next;
                 if(cur->tok_value != K_FROM)
                 {

                     rc = INVALID_SYNTAX;
                     cur->tok_value = INVALID;
                     return rc;
                 }
                 else
                 {
                     cur = cur->next;
                     if ((cur->tok_class != keyword) &&
                           (cur->tok_class != identifier) &&
                             (cur->tok_class != type_name))
                     {
                         // Error
                         rc = INVALID_TABLE_NAME;
                         cur->tok_value = INVALID;
                         return rc;
                     }
                     else
                     {
                         if ((new_entry = get_tpd_from_list(cur->tok_string)) == NULL)
                         {
                             rc = TABLE_NOT_EXIST;
                             cur->tok_value = INVALID;
                             return rc;
                         }
                         else
                         {
                             strcpy(tab_name,cur->tok_string);
                             temp_entry=get_tpd_from_list(tab_name);
                             col_entry = (cd_entry*)((char*)temp_entry + temp_entry->cd_offset);
                             for(int i=0;i<temp_entry->num_columns;i++)
                             {
                                 if(strcasecmp(col_entry[i].col_name,aggr_parameter)==0)
                                 {
                                     found = true;
                                     j=i;
                                 }
                             }
                             if(!found)
                             {
                                 rc = INVALID_COLUMN_NAME;
                                 temp->tok_value = INVALID;
                                 return rc;
                             }
                             if(col_entry[j].col_type!=T_INT)
                             {
                                 rc = INVALID_AGGREGATE_FUNCTION_PARAMETER;
                                 temp->tok_value = INVALID;
                                 return rc;
                             }
                             // 100 here is max records.
                             for ( int cur_record = 0; cur_record < 100; cur_record++ ){
                                 records[cur_record].first = 1;
                             }
                         }
                     }
                    }
                }
            }
        }
        else if(cur->tok_value == F_COUNT)
        {
            agg_fn = cur->tok_value;
            cur = cur->next;
            if(cur->tok_value!=S_LEFT_PAREN)
            {
                rc = INVALID_SYNTAX;
                cur->tok_value = INVALID;
                return rc;
            }
            else
            {
                cur = cur->next;
                if(cur->tok_value!=IDENT && cur->tok_value!= S_STAR)
                {
                    rc = INVALID_SYNTAX;
                    cur->tok_value = INVALID;
                    return rc;
                }
                else
                {
                    if(cur->tok_value ==IDENT)
                    {
                     strcpy(aggr_parameter,cur->tok_string);
                        temp = cur;
                    }
                    if(cur->tok_value==S_STAR)
                    {
                        count_is_star = true;
                    }
                    cur = cur->next;
                    if(cur->tok_value!=S_RIGHT_PAREN)
                    {
                        rc = INVALID_SYNTAX;
                        cur->tok_value = INVALID;
                        return rc;
                    }
                    else
                    {
                        cur = cur->next;
                        if(cur->tok_value != K_FROM)
                        {
                            rc = INVALID_SYNTAX;
                            cur->tok_value = INVALID;
                            return rc;
                        }
                        else
                        {
                            cur = cur->next;
                            if ((cur->tok_class != keyword) &&
                                  (cur->tok_class != identifier) &&
                                    (cur->tok_class != type_name))
                            {
                                // Error
                                rc = INVALID_TABLE_NAME;
                                cur->tok_value = INVALID;
                                return rc;
                            }
                            else
                            {
                                if ((new_entry = get_tpd_from_list(cur->tok_string)) == NULL)
                                {
                                    rc = TABLE_NOT_EXIST;
                                    cur->tok_value = INVALID;
                                    return rc;
                                }
                                else
                                {
                                    strcpy(tab_name,cur->tok_string);
                                    temp_entry=get_tpd_from_list(tab_name);
                                    col_entry = (cd_entry*)((char*)temp_entry + temp_entry->cd_offset);
                                    for(int i=0;i<temp_entry->num_columns;i++)
                                    {
                                        if(strcasecmp(col_entry[i].col_name,aggr_parameter)==0)
                                        {
                                            found = true;
                                            j=i;
                                        }
                                    }
                                    if(!count_is_star && !found)
                                    {
                                        rc = INVALID_COLUMN_NAME;
                                        temp->tok_value = INVALID;
                                        return rc;
                                    }
                                    // 100 here is max records.
                                    for ( int cur_record = 0; cur_record < 100; cur_record++ ){
                                        records[cur_record].first = 1;
                                    }
                                }
                            }
                        }
        
                    }
                }
            }
        }
else if(cur->tok_value == IDENT)
{
    int i=0;
    temp=cur;
    do{
        if(cur->next->tok_value == S_DOT)
        {
            
            strcpy(select_table[i],cur->tok_string);
            cur = cur->next->next;
        }
        strcpy(to_print[i],cur->tok_string);
        num = num+1;
       if(cur->next->tok_value == S_COMMA)
        {
            cur = cur->next;
            if(cur->next->tok_value==K_FROM)
            {
                rc = INVALID_SYNTAX;
                cur->tok_value = INVALID;
                break;
            }
            else
            {
                cur=cur->next;
                if(cur->tok_value== F_SUM || cur->tok_value == F_AVG || cur->tok_value == F_COUNT)
                {
                    if(cur->tok_value== F_SUM || cur->tok_value == F_AVG )
                    {
                        group_agg_fn = cur->tok_value;
                        check = cur;
                        cur = cur->next;
                        if(cur->tok_value!=S_LEFT_PAREN)
                        {

                            rc = INVALID_SYNTAX;
                            cur->tok_value = INVALID;
                            return rc;
                        }
                        else
                        {
                            cur = cur->next;
                            if(cur->tok_value != IDENT)
                            {
                                rc = INVALID_SYNTAX;
                                cur->tok_value = INVALID;
                                return rc;
                            }
                            else
                            {
                                strcpy(group_agg_col,cur->tok_string);
                                group_temp = cur;
                            }
                            cur = cur->next;
                            if(cur->tok_value!=S_RIGHT_PAREN)
                            {

                                rc = INVALID_SYNTAX;
                                cur->tok_value = INVALID;
                                return rc;
                            }
                            else
                            {
                                cur = cur->next;
                            }
                        }
                    }
                    else if(cur->tok_value == F_COUNT)
                    {
                        group_agg_fn = cur->tok_value;
                        check = cur;
                        cur = cur->next;
                        if(cur->tok_value!=S_LEFT_PAREN)
                        {
                            rc = INVALID_SYNTAX;
                            cur->tok_value = INVALID;
                            return rc;
                        }
                        else
                        {
                            cur = cur->next;
                            if(cur->tok_value!=IDENT && cur->tok_value!= S_STAR)
                            {
                                rc = INVALID_SYNTAX;
                                cur->tok_value = INVALID;
                                return rc;
                            }
                            else
                            {
                                if(cur->tok_value ==IDENT)
                                {
                                 strcpy(group_agg_col,cur->tok_string);
                                    group_temp = cur;
                                }
                                else if(cur->tok_value==S_STAR)
                                {
                                    group_count_is_star = true;
                                }
                                cur = cur->next;
                                if(cur->tok_value!=S_RIGHT_PAREN)
                                {
                                    rc = INVALID_SYNTAX;
                                    cur->tok_value = INVALID;
                                    return rc;
                                }
                                else
                                {
                                    cur = cur->next;
                                }
                            }
                        }
                    }
                }
                
            }
        }
        else if(cur->next->tok_value == K_FROM)
        {
            cur = cur->next;
        }
        else
        {
            rc = INVALID_SYNTAX;
            cur->tok_value = INVALID;
            break;
        }
        i=i+1;
    }while(cur->tok_value!=K_FROM);
    if(cur->tok_value!=K_FROM)
    {
        rc = INVALID_SYNTAX;
        cur->tok_value = INVALID;
        return rc;
    }
    else
    {
        cur = cur->next;
        if ((cur->tok_class != keyword) &&
              (cur->tok_class != identifier) &&
                (cur->tok_class != type_name))
        {
            // Error
            rc = INVALID_TABLE_NAME;
            cur->tok_value = INVALID;
            return rc;
        }
        else
        {
            if ((new_entry = get_tpd_from_list(cur->tok_string)) == NULL)
            {
                rc = TABLE_NOT_EXIST;
                cur->tok_value = INVALID;
                return rc;
            }
            else
            {
                strcpy(tab_name,cur->tok_string);
                temp_entry=get_tpd_from_list(tab_name);
                col_entry = (cd_entry*)((char*)temp_entry + temp_entry->cd_offset);
                
                if (cur->next->tok_value == K_NATURAL)
                {
                    cur = cur->next->next;
                    if(cur->tok_value != K_JOIN)
                    {
                        rc = INVALID_SYNTAX;
                        cur->tok_value = INVALID;
                        return rc;
                    }
                    else
                    {
                        cur = cur->next;
                        if ((cur->tok_class != keyword) &&
                              (cur->tok_class != identifier) &&
                                (cur->tok_class != type_name))
                        {
                            // Error
                            rc = INVALID_TABLE_NAME;
                            cur->tok_value = INVALID;
                            return rc;
                        }
                        else
                        {
                            if ((new_entry1 = get_tpd_from_list(cur->tok_string)) == NULL)
                            {
                                rc = TABLE_NOT_EXIST;
                                cur->tok_value = INVALID;
                                return rc;
                            }
                            else
                            {
                                strcpy(tab_name1,cur->tok_string);
                                temp_entry1=get_tpd_from_list(tab_name1);
                                col_entry1 = (cd_entry*)((char*)temp_entry1 + temp_entry1->cd_offset);
                               for(int a=0;a<num;a++)
                               {
                                   if(strcasecmp(select_table[a],"o")==0)
                                   {
                                       strcpy(select_table[a],tab_name);
                                   }
                                   if((strcasecmp(select_table[a],tab_name)!=0) && (strcasecmp(select_table[a],tab_name1)!=0))
                                   {
                                       
                                       rc = INVALID_TABLE_NAME;
                                       cur->tok_value = INVALID;
                                       return rc;
                                   }
                                   else
                                   {
                                   temp_entry2=get_tpd_from_list(select_table[a]);
                                   col_entry2 = (cd_entry*)((char*)temp_entry2 + temp_entry2->cd_offset);
                                    for(int c=0;c<temp_entry2->num_columns;c++)
                                    {
                                        if(strcasecmp(col_entry2[c].col_name,to_print[a])==0)
                                        {
                                            found1 = true;
                                            
                                            index[a]=c;
                                            col_to_print++;
                                        }
                                    }
                                       if(!found1)
                                       {
                                           rc = COLUMN_NOT_EXIST;
                                           cur->tok_value = INVALID;
                                           return rc;
                                       }
                                       found1 =false;
                                   }
                               }
                                
                                for (int i=0;i<temp_entry->num_columns;i++)
                                {
                                    for(int j =0;j<temp_entry1->num_columns;j++)
                                    {
                                        if(strcasecmp(col_entry[i].col_name,col_entry1[j].col_name)==0)
                                        {
                                            strcpy(shared_columns[i],col_entry[i].col_name);
                                            num_shared_columns++;
                                        }
                                        
                                    }
                                }
                            }
                        }
                        
                    }
                }
                else
                {
                for(int a=0;a<num;a++)
                {
                    for(int b=0;b<temp_entry->num_columns;b++)
                    {
                        if((strcasecmp(col_entry[b].col_name,to_print[a])==0))
                        {
                            found = true;
                            temp = temp->next->next;
                            index[a]=b;
                            col_to_print++;
                        }
                    }
                    if(!found)
                    {
                        rc = COLUMN_NOT_EXIST;
                        temp->tok_value = INVALID;
                        return rc;
                    }
                    found =false;
                }
                if(group_agg_fn == F_COUNT)
                {
                    for(int x=0;x<temp_entry->num_columns;x++)
                    {
                        if(strcasecmp(col_entry[x].col_name,group_agg_col)==0)
                        {
                            group_col_found = true;
                            group_col_index=x;
                        }
                    }
                    if(!group_count_is_star && !group_col_found)
                    {
                        rc = INVALID_COLUMN_NAME;
                        group_temp->tok_value = INVALID;
                        return rc;
                    }
                }
                else if(group_agg_fn == F_SUM || group_agg_fn == F_AVG)
                {
                    for(int x=0;x<temp_entry->num_columns;x++)
                    {
                        if(strcasecmp(col_entry[x].col_name,group_agg_col)==0)
                        {
                            group_col_found = true;
                            group_col_index=x;
                        }
                    }
                    if(!group_col_found)
                    {
                        
                        rc = INVALID_COLUMN_NAME;
                        group_temp->tok_value = INVALID;
                        return rc;
                    }
                    if(col_entry[group_col_index].col_type!=T_INT)
                    {
                        rc = INVALID_AGGREGATE_FUNCTION_PARAMETER;
                        group_temp->tok_value = INVALID;
                        return rc;
                    }
                }
                
            }
        }
    }
    }
    
    // 100 here is max records.
    for ( int cur_record = 0; cur_record < 100; cur_record++ ){
        records[cur_record].first = 1;
    }
}
else if(cur->tok_value == S_STAR)
{
   
    cur = cur->next;
    if(cur->tok_value != K_FROM)
    {
        rc = INVALID_SYNTAX;
        cur->tok_value = INVALID;
        return rc;
    }
    else
    {
        cur = cur->next;
        if ((cur->tok_class != keyword) &&
              (cur->tok_class != identifier) &&
                (cur->tok_class != type_name))
        {
            // Error
            rc = INVALID_TABLE_NAME;
            cur->tok_value = INVALID;
            return rc;
        }
        else
        {
            if ((new_entry = get_tpd_from_list(cur->tok_string)) == NULL)
            {
                rc = TABLE_NOT_EXIST;
                cur->tok_value = INVALID;
                return rc;
            }
            else
            {
                strcpy(tab_name,cur->tok_string);
                temp_entry=get_tpd_from_list(tab_name);
                col_entry = (cd_entry*)((char*)temp_entry + temp_entry->cd_offset);
                if(cur->next->tok_value == K_NATURAL)
                {
                    cur = cur->next->next;
                    if(cur->tok_value != K_JOIN)
                    {
                        rc = INVALID_SYNTAX;
                        cur->tok_value = INVALID;
                        return rc;
                    }
                    else
                    {
                        cur = cur->next;
                        if ((cur->tok_class != keyword) &&
                              (cur->tok_class != identifier) &&
                                (cur->tok_class != type_name))
                        {
                            // Error
                            rc = INVALID_TABLE_NAME;
                            cur->tok_value = INVALID;
                            return rc;
                        }
                        else
                        {
                            if ((new_entry1 = get_tpd_from_list(cur->tok_string)) == NULL)
                            {
                                rc = TABLE_NOT_EXIST;
                                cur->tok_value = INVALID;
                                return rc;
                            }
                            else
                            {
                                strcpy(tab_name1,cur->tok_string);
                                temp_entry1=get_tpd_from_list(tab_name1);
                                col_entry1 = (cd_entry*)((char*)temp_entry1 + temp_entry1->cd_offset);
                                col_to_print = temp_entry->num_columns + temp_entry1->num_columns -1;
                               }
                            }
                        }
                    }
                else{
                for(int b=0;b<temp_entry->num_columns;b++)
                {
                        index[b]=b;
                        col_to_print++;
                }
                
                // 100 here is max records.
                for ( int cur_record = 0; cur_record < 100; cur_record++ ){
                    records[cur_record].first = 1;
                }
                }
            }
        }
        
    }
}

}
if(cur->next->tok_value != EOC)
{
    bool group_found = false;
    strcpy(file_name, tab_name);
    strcat(file_name,".tab");
    fhandle = fopen(file_name, "rbc");
    table_file_header* temp_header = NULL;
    temp_header = (table_file_header*)calloc(1, sizeof(table_file_header));
    fread(temp_header,sizeof(table_file_header), 1, fhandle);
    fflush(fhandle);
    fclose(fhandle);
    int cur_records = 0;
    cur = cur->next;
    if(cur->tok_value!= K_WHERE && cur->tok_value !=K_ORDER && cur->tok_value != K_GROUP)
    {
        rc = INVALID_SYNTAX;
        cur->tok_value = INVALID;
        return rc;
    }
    else
    {
        if(cur->tok_value == K_GROUP)
        {
            check_group = cur;
               cur = cur->next;
                if(cur->tok_value!=K_BY)
                {
                    rc = INVALID_SYNTAX;
                    cur->tok_value = INVALID;
                    return rc;
                }
                else
                {
                    group = true;
                    group_found = false;
                    cur = cur->next;
                    for(int i=0;i<num;i++)
                    {
                        if(strcasecmp(to_print[i],cur->tok_string)==0)
                        {
                                group_found = true;
                        }
                    }
                    if(!group_found)
                    {
                        rc =INVALID_COLUMN_NAME;
                        cur->tok_value = INVALID;
                        return rc;
                    }
                    else
                    {
                        for(int i=0;i<temp_entry->num_columns;i++)
                        {
                            if(strcasecmp(col_entry[i].col_name,cur->tok_string)==0)
                            {
                                    index_group=i;
                            }
                        }
                        strcpy(group_col,cur->tok_string);
                        if(cur->next->tok_value !=EOC && cur->next->tok_value != K_ORDER )
                        {
                            rc =INVALID_STATEMENT;
                            cur->next->tok_value = INVALID;
                            return rc;
                                }
                            }
                         }
                    }
       else if(cur->tok_value ==K_ORDER)
        {
            cur = cur->next;
                if(cur->tok_value!=K_BY)
                {
                    rc = INVALID_SYNTAX;
                    cur->tok_value = INVALID;
                    return rc;
                }
                else
                {
                    found = false;
                    cur = cur->next;
                    for(int i=0;i<temp_entry->num_columns;i++)
                    {
                        if(strcasecmp(col_entry[i].col_name,cur->tok_string)==0)
                        {
                                found = true;
                                index_order=i;
                        }
                    }
                    if(!found)
                    {
                        rc =INVALID_COLUMN_NAME;
                        cur->tok_value = INVALID;
                        return rc;
                    }
                    else
                    {
                    strcpy(check_column_orderby,cur->tok_string);
                    if(cur->next->tok_value !=EOC)
                    {
                        cur = cur->next;
                        if(cur->tok_value != K_DESC)
                        {
                            rc =INVALID_SYNTAX;
                            cur->tok_value = INVALID;
                            return rc;
                        }
                        else
                        {
                            isdesc = true;
                            if(cur->next->tok_value!=EOC)
                            {
                                rc =INVALID_STATEMENT;
                                cur->tok_value = INVALID;
                                return rc;
                            }
                        }
                     }
                        
                        strcpy(file_name, tab_name);
                        strcat(file_name,".tab");
                        get_record_order(records, file_name, col_entry, index_order, order, !isdesc);
                        is_ordered = true;
                    }
                }
            }
        else if(cur->tok_value == K_WHERE)
        {
            if(cur->next->tok_value == K_NOT)
            {
                isnot1 = true;
                cur= cur->next;
            }
        cur = cur->next;
        if(cur->next->tok_value == S_DOT)
        {
            strcpy(table_where_first,cur->tok_string);
            if((strcasecmp(table_where_first,tab_name)!=0) && (strcasecmp(table_where_first,tab_name1)!=0))
            {
                rc = INVALID_TABLE_NAME;
                cur->tok_value = INVALID;
                return rc;
            }
            else
            {
            cur = cur->next->next;
            }
        }
       else
       {
           strcpy(table_where_first,tab_name);
        }
            temp_entry2=get_tpd_from_list(table_where_first);
            col_entry2 = (cd_entry*)((char*)temp_entry2 + temp_entry2->cd_offset);
        for(int i=0;i<temp_entry2->num_columns;i++)
        {
            if(strcasecmp(col_entry2[i].col_name,cur->tok_string)==0)
            {
                    found = true;
                    index_one=i;
            }
        }
        if(!found)
        {
            rc =INVALID_COLUMN_NAME;
            cur->tok_value = INVALID;
            return rc;
        }
        strcpy(check_column1,cur->tok_string);
        cur = cur->next;
        if(cur->tok_value != S_GREATER && cur->tok_value != S_LESS && cur->tok_value != S_EQUAL && cur->tok_value != K_IS)
        {
            rc =INVALID_SYNTAX;
            cur->tok_value = INVALID;
            return rc;
        }
        else
        {
            if(cur->tok_value == S_GREATER || cur->tok_value == S_LESS || cur->tok_value == S_EQUAL)
            {
                rel1 = cur->tok_value;
                cur = cur->next;
                if(col_entry2[index_one].col_type == T_INT && cur->tok_value != INT_LITERAL && cur->tok_value != K_NULL)
                {
                    rc = DATA_MISMATCH;
                    cur->tok_value = INVALID;
                    return rc;
                }
                else if(col_entry2[index_one].col_type == T_CHAR && cur->tok_value != STRING_LITERAL && cur->tok_value != K_NULL)
                {
                    rc = DATA_MISMATCH;
                    cur->tok_value = INVALID;
                    return rc;
                }
                else
                {
                    if(cur->tok_value == INT_LITERAL)
                    {
                        if(atoi(cur->tok_string)<0)
                        {
                            rc = INVALID_COLUMN_ENTRY;
                            cur->tok_value = INVALID;
                            return rc;
                        }
                        else
                        {
                            val1 = atoi(cur->tok_string);
                            get_list(records, file_name, col_entry2, index_one, rel1, cur->tok_string, 1);
                            
                            if (isnot1){
                                negate_records(records,temp_header->num_records);
                            }
                            
                        }
                    }
                    else if(cur->tok_value == K_NULL)
                    {
                        get_list(records, file_name, col_entry2, index_one, rel1, cur->tok_string, -1);
                        
                        if (isnot1){
                            negate_records(records,temp_header->num_records);
                        }
                        
                    }
                    else if(cur->tok_value == STRING_LITERAL)
                    {
                        copy_size1 = strlen(cur->tok_string);
                        if(copy_size1 > col_entry2[index_one].col_len )
                        {
                            rc = INVALID_COLUMN_ENTRY;
                            cur->tok_value = INVALID;
                            return rc;
                        }
                        else
                        {
                            get_list(records, file_name, col_entry2, index_one, rel1, cur->tok_string, 0);
                            
                            if (isnot1){
                                negate_records(records,temp_header->num_records);
                            }
                        }
                    }
                }
            }
            else if(cur->tok_value == K_IS)
            {
                cur = cur->next;
                if(cur->tok_value!= K_NOT && cur->tok_value!= K_NULL)
                {
                    rc =INVALID_SYNTAX;
                    cur->tok_value = INVALID;
                    return rc;
                }
                else
                {
                    if(cur->tok_value == K_NOT)
                    {
                        cur = cur->next;
                        if(cur->tok_value!= K_NULL)
                        {
                            rc =INVALID_SYNTAX;
                            cur->tok_value = INVALID;
                            return rc;
                        }
                        else
                        {
                            isnotnull1 = true;
                            get_list(records, file_name, col_entry2, index_one, K_NOT, cur->tok_string, -1);
                            
                            if (isnot1){
                                negate_records(records,temp_header->num_records);
                            }

                        }
                    }
                   else if(cur->tok_value == K_NULL)
                    {
                        isnull1 = true;
                        if(col_entry2[index_one].not_null == 1 && isnull1 == true)
                        {
                            rc = NULL_VALUE_INSERTED;
                            cur->tok_value = INVALID;
                            return rc;
                        }
                        get_list(records, file_name, col_entry2, index_one, K_IS, cur->tok_string, -1);
                        
                        if (isnot1){
                            negate_records(records,temp_header->num_records);
                        }
                    }
                }
            }
        }
    }
}
 if(cur->next->tok_value != EOC)
 {
    
     cur = cur->next;
     if(cur->tok_value != K_AND && cur->tok_value != K_OR && cur->tok_value != K_ORDER && cur->tok_value != K_GROUP)
     {
         rc =INVALID_SYNTAX;
         cur->tok_value = INVALID;
         return rc;
     }
     else
     {
         if(cur->tok_value == K_GROUP)
         {
             check_group = cur;
             cur = cur->next;
                 if(cur->tok_value!=K_BY)
                 {
                     rc = INVALID_SYNTAX;
                     cur->tok_value = INVALID;
                     return rc;
                 }
                 else
                 {
                     group = true;
                     group_found = false;
                     cur = cur->next;
                     for(int i=0;i<num;i++)
                     {
                         if(strcasecmp(to_print[i],cur->tok_string)==0)
                         {
                                 group_found = true;
                         }
                     }
                     if(!group_found)
                     {
                         rc =INVALID_COLUMN_NAME;
                         cur->tok_value = INVALID;
                         return rc;
                     }
                     else
                     {
                         for(int i=0;i<temp_entry->num_columns;i++)
                         {
                             if(strcasecmp(col_entry[i].col_name,cur->tok_string)==0)
                             {
                                     index_group=i;
                             }
                         }
                         strcpy(group_col,cur->tok_string);
                         if(cur->next->tok_value !=EOC && cur->next->tok_value != K_ORDER )
                         {
                             rc =INVALID_STATEMENT;
                             cur->next->tok_value = INVALID;
                             return rc;
                        }
                     }
                     
                 }
         }
         else if(cur->tok_value == K_ORDER)
         {
                cur = cur->next;
                 if(cur->tok_value!=K_BY)
                 {
                     rc = INVALID_SYNTAX;
                     cur->tok_value = INVALID;
                     return rc;
                 }
                 else
                 {
                     found = false;
                     cur = cur->next;
                     for(int i=0;i<temp_entry->num_columns;i++)
                     {
                         if(strcasecmp(col_entry[i].col_name,cur->tok_string)==0)
                         {
                                 found = true;
                                 index_order=i;
                         }
                     }
                     if(!found)
                     {
                         rc =INVALID_COLUMN_NAME;
                         cur->tok_value = INVALID;
                         return rc;
                     }
                     else
                     {
                     strcpy(check_column_orderby,cur->tok_string);
                     if(cur->next->tok_value !=EOC)
                     {
                         cur = cur->next;
                         if(cur->tok_value != K_DESC)
                         {
                             rc =INVALID_SYNTAX;
                             cur->tok_value = INVALID;
                             return rc;
                         }
                         else
                         {
                             isdesc = true;
                             if(cur->next->tok_value!=EOC)
                             {
                                 rc =INVALID_STATEMENT;
                                 cur->tok_value = INVALID;
                                 return rc;
                             }
                         }
                      }
                         
                         strcpy(file_name, tab_name);
                         strcat(file_name,".tab");
                         get_record_order(records, file_name, col_entry, index_order, order, !isdesc);
                         is_ordered = true;
                     }
                 }
     }
     else
     {
         if(strcasecmp("AND",cur->tok_string)==0)
         {
             cond = 1;
         }
         else
         {
             cond=2;
         }
         if(cur->next->tok_value == K_NOT)
         {
             isnot2 = true;
             cur= cur->next;
         }
         found = false;
         cur = cur->next;
         if(cur->next->tok_value == S_DOT)
         {
             strcpy(table_where_second,cur->tok_string);
             if((strcasecmp(table_where_second,tab_name)!=0) && (strcasecmp(table_where_second,tab_name1)!=0))
             {
                 rc = INVALID_TABLE_NAME;
                 cur->tok_value = INVALID;
                 return rc;
             }
             else
             {
             cur = cur->next->next;
             }
         }
        else
        {
            strcpy(table_where_second,tab_name);
         }
             temp_entry3=get_tpd_from_list(table_where_second);
             col_entry3 = (cd_entry*)((char*)temp_entry3 + temp_entry3->cd_offset);
         for(int i=0;i<temp_entry3->num_columns;i++)
         {
             if(strcasecmp(col_entry3[i].col_name,cur->tok_string)==0)
             {
                     found = true;
                     index_two=i;
             }
         }
         if(!found)
         {
             rc =INVALID_COLUMN_NAME;
             cur->tok_value = INVALID;
             return rc;
         }
         strcpy(check_column2,cur->tok_string);
         cur = cur->next;
         if(cur->tok_value != S_GREATER && cur->tok_value != S_LESS && cur->tok_value != S_EQUAL && cur->tok_value != K_IS)
         {
             rc =INVALID_SYNTAX;
             cur->tok_value = INVALID;
             return rc;
         }
         else
         {
             if(cur->tok_value == S_GREATER || cur->tok_value == S_LESS || cur->tok_value == S_EQUAL)
             {
                 rel2 = cur->tok_value;
                 cur = cur->next;
                 if(col_entry3[index_two].col_type == T_INT && cur->tok_value != INT_LITERAL && cur->tok_value != K_NULL)
                 {
                     rc = DATA_MISMATCH;
                     cur->tok_value = INVALID;
                     return rc;
                 }
                 else if(col_entry3[index_two].col_type == T_CHAR && cur->tok_value != STRING_LITERAL && cur->tok_value != K_NULL)
                 {
                     rc = DATA_MISMATCH;
                     cur->tok_value = INVALID;
                     return rc;
                 }
                 else if(col_entry[index_two].not_null == 1 && cur->tok_value == K_NULL)
                 {
                     rc = NULL_VALUE_INSERTED;
                     cur->tok_value = INVALID;
                     return rc;
                 }
                 else
                 {
                     if(cur->tok_value == INT_LITERAL)
                     {
                         if(atoi(cur->tok_string)<0)
                         {
                             rc = INVALID_COLUMN_ENTRY;
                             cur->tok_value = INVALID;
                             return rc;
                         }
                         else
                         {
                             val2 = atoi(cur->tok_string);
                             std::pair<int,char[]> temp_records[temp_header->num_records];

                             get_list(temp_records, file_name, col_entry3, index_two, rel2, cur->tok_string, 1);
                             
                             if (isnot2){
                                 negate_records(temp_records,temp_header->num_records);
                             }
                             
                             if (cond == 1){
                                 merge_records_and(records,temp_records,temp_header->num_records);
                             }else {
                                 merge_records_or(records,temp_records,temp_header->num_records);
                             }
                         }
                     }
                     else if(cur->tok_value == K_NULL)
                     {
                         std::pair<int,char[]> temp_records[temp_header->num_records];
                         get_list(temp_records, file_name, col_entry3, index_two, rel2, cur->tok_string, -1);
                         
                         if (isnot2){
                             negate_records(temp_records,temp_header->num_records);
                         }
                         
                         if (cond == 1){
                             merge_records_and(records,temp_records,temp_header->num_records);
                         }else {
                             merge_records_or(records,temp_records,temp_header->num_records);
                         }
                         
                         
                     }
                     else if(cur->tok_value == STRING_LITERAL)
                     {
                         copy_size1 = strlen(cur->tok_string);
                         if(copy_size1 > col_entry3[index_one].col_len )
                         {
                             rc = INVALID_COLUMN_ENTRY;
                             cur->tok_value = INVALID;
                             return rc;
                         }
                         else
                         {
                             std::pair<int,char[]> temp_records[temp_header->num_records];

                             get_list(temp_records, file_name, col_entry3, index_two, rel2, cur->tok_string, 0);
                             
                             if (isnot2){
                                 negate_records(temp_records,temp_header->num_records);
                             }
                             
                             if (cond == 1){
                                 merge_records_and(records,temp_records,temp_header->num_records);
                             }else {
                                 merge_records_or(records,temp_records,temp_header->num_records);
                             }
                         }
                     }
                 }
             }
             if(cur->tok_value == K_IS)
             {
                 cur = cur->next;
                 if(cur->tok_value!= K_NOT && cur->tok_value!= K_NULL)
                 {
                     rc =INVALID_SYNTAX;
                     cur->tok_value = INVALID;
                     return rc;
                 }
                 else
                 {
                     if(cur->tok_value == K_NOT)
                     {
                         cur = cur->next;
                         if(cur->tok_value!= K_NULL)
                         {
                             rc =INVALID_SYNTAX;
                             cur->tok_value = INVALID;
                             return rc;
                         }
                         else
                         {
                             isnotnull2 = true;
                             std::pair<int,char[]> temp_records[temp_header->num_records];
                             get_list(temp_records, file_name, col_entry3, index_two, K_NOT, cur->tok_string, -1);
                             
                             if (isnot2){
                                 negate_records(temp_records,temp_header->num_records);
                             }
                             
                             if (cond == 1){
                                 merge_records_and(records,temp_records,temp_header->num_records);
                             }else {
                                 merge_records_or(records,temp_records,temp_header->num_records);
                             }
                         }
                     }
                     else if(cur ->tok_value == K_NULL)
                     {
                         isnull2 = true;
                         if(col_entry3[index_two].not_null == 1 && isnull2 == true)
                         {
                             rc = NULL_VALUE_INSERTED;
                             cur->tok_value = INVALID;
                             return rc;
                         }
                         std::pair<int,char[]> temp_records[temp_header->num_records];
                         get_list(temp_records, file_name, col_entry3, index_two, K_IS, cur->tok_string, -1);
                         
                         if (isnot2){
                             negate_records(temp_records,temp_header->num_records);
                         }

                         if (cond == 1){
                             merge_records_and(records,temp_records,temp_header->num_records);
                         }else {
                             merge_records_or(records,temp_records,temp_header->num_records);
                         }
                     }
                 }
             }
         }
         
     }
 }
 }
if(cur->next->tok_value != EOC)
{
    
    cur = cur->next;
    if(cur->tok_value != K_ORDER && cur->tok_value != K_GROUP)
    {
        rc = INVALID_SYNTAX;
        cur->tok_value = INVALID;
        return rc;
    }
    else
    {
    if(cur->tok_value == K_GROUP)
        {
            check_group = cur;
            cur = cur->next;
                if(cur->tok_value!=K_BY)
                {
                    rc = INVALID_SYNTAX;
                    cur->tok_value = INVALID;
                    return rc;
                }
                else
                {
                    group = true;
                    group_found = false;
                    cur = cur->next;
                    for(int i=0;i<num;i++)
                    {
                        if(strcasecmp(to_print[i],cur->tok_string)==0)
                        {
                                group_found = true;
                        }
                    }
                    if(!group_found)
                    {
                        rc =INVALID_COLUMN_NAME;
                        cur->tok_value = INVALID;
                        return rc;
                    }
                    else
                    {
                        for(int i=0;i<temp_entry->num_columns;i++)
                        {
                            if(strcasecmp(col_entry[i].col_name,cur->tok_string)==0)
                            {
                                    index_group=i;
                            }
                        }
                        strcpy(group_col,cur->tok_string);
                        if(cur->next->tok_value != EOC)
                        {
                            cur= cur->next;
                        }
                    }
                }
        }
        
    if(cur->next->tok_value != EOC)
    {
    if(cur->tok_value != K_ORDER)
    {
        rc = INVALID_SYNTAX;
        cur->tok_value = INVALID;
        return rc;
    }
    else
    {
        cur = cur->next;
        if(cur->tok_value!=K_BY)
        {
            rc = INVALID_SYNTAX;
            cur->tok_value = INVALID;
            return rc;
        }
        else
        {
            found = false;
            cur = cur->next;
            for(int i=0;i<temp_entry->num_columns;i++)
            {
                if(strcasecmp(col_entry[i].col_name,cur->tok_string)==0)
                {
                        found = true;
                        index_order=i;
                }
            }
            if(!found)
            {
                rc =INVALID_COLUMN_NAME;
                cur->tok_value = INVALID;
                return rc;
            }
            else
            {
            strcpy(check_column_orderby,cur->tok_string);
            if(cur->next->tok_value !=EOC)
            {
                cur = cur->next;
                if(cur->tok_value != K_DESC)
                {
                    rc =INVALID_SYNTAX;
                    cur->tok_value = INVALID;
                    return rc;
                }
                else
                {
                    isdesc = true;
                    if(cur->next->tok_value!=EOC)
                    {
                        rc =INVALID_STATEMENT;
                        cur->tok_value = INVALID;
                        return rc;
                    }
                }
             }
                strcpy(file_name, tab_name);
                strcat(file_name,".tab");
                get_record_order(records, file_name, col_entry, index_order, order, !isdesc);
                is_ordered = true;
            }
        }
    }
}
}
}

}
    //pranavi
    if (group == true && group_agg_fn != -1)
    {
        if ( is_ordered ){
            print_group_by(index, file_name, records, col_entry, col_to_print, order, group_agg_fn, group_col_index, index_group);
        }
        else {
            print_group_by(index, file_name, records, col_entry, col_to_print, NULL, group_agg_fn, group_col_index, index_group);
        }
                
    }
    else if ((group_agg_fn != -1) && (group == false))
    {
        rc =INVALID_STATEMENT;
        check->tok_value = INVALID;
        return rc;
       }
    else if((group_agg_fn == -1) && (group == true))
        {
            rc =INVALID_STATEMENT;
            check_group->tok_value = INVALID;
            return rc;
        }
    
    else if ( agg_fn == F_SUM ) {
        strcpy(file_name, tab_name);
        strcat(file_name,".tab");
        print_sum(j,file_name, records, col_entry);
        
    }
    else if (agg_fn == F_COUNT ) {
        strcpy(file_name, tab_name);
        strcat(file_name,".tab");
        if (count_is_star){
            // -1 is *
            j = -1;
        }
        print_count(j,file_name, records, col_entry);
    }
    else if (agg_fn == F_AVG){
        strcpy(file_name, tab_name);
        strcat(file_name,".tab");
        print_avg(j,file_name, records, col_entry);
    }
    else {
        strcpy(file_name, tab_name);
        strcat(file_name,".tab");
        if ( is_ordered ){
            print_rows(index, file_name, records,col_entry, col_to_print, order);
        }
        else {
            print_rows(index, file_name, records,col_entry, col_to_print, NULL);
        }
    }
   
return rc;
}
int sem_insert_into(token_list *t_list)
{
        int rc = 0,i,size,remainder,temp=0,rec_size,x=0,j=0;
        token_list *cur;
        char tab_name[MAX_IDENT_LEN+4];
        tpd_entry *new_entry = NULL;
        tpd_entry *temp_entry =NULL;
        cd_entry  *col_entry = NULL;
    table_file_header *new1_header=NULL;
    FILE *fhandle = NULL;
    char file_name[MAX_IDENT_LEN+4];
    cur = t_list;
    if ((cur->tok_class != keyword) &&
          (cur->tok_class != identifier) &&
            (cur->tok_class != type_name))
    {
        // Error
        rc = INVALID_TABLE_NAME;
        cur->tok_value = INVALID;
    }
      else if ((new_entry = get_tpd_from_list(cur->tok_string)) == NULL)
        {
            
            rc = TABLE_NOT_EXIST;
            cur->tok_value = INVALID;
        }
        else
        {   strcpy(tab_name,cur->tok_string);
            cur = cur->next;
            if (cur->tok_value != K_VALUES)
            {
                //Error
                rc = INVALID_SYNTAX;
                cur->tok_value = INVALID;
            }
            else
            {
                cur = cur->next;
                if (cur->tok_value != S_LEFT_PAREN)
                {
                    //Error
                    rc = INVALID_SYNTAX;
                    cur->tok_value = INVALID;
                }
                else
                {
                    strcpy(file_name,tab_name);
                    strcat(file_name,".tab");
                    fhandle = fopen(file_name, "rbc");
                    temp_entry=get_tpd_from_list(tab_name);
                    col_entry = (cd_entry*)((char*)temp_entry + temp_entry->cd_offset);
                    for(i=0;i<temp_entry->num_columns;i++)
                    {
                        temp = temp + col_entry[i].col_len;
                    }
                    size = temp + temp_entry->num_columns;
                    remainder = size % 4;
                        if (remainder == 0)
                        { rec_size = size;}
                        else
                        { rec_size = size + 4 - remainder;}
                    new1_header = (table_file_header*)malloc((100 * rec_size)+ sizeof(table_file_header));
                    fread(new1_header,sizeof(table_file_header)+(100 * rec_size), 1, fhandle);
                    fhandle = fopen(file_name, "wbc");
                    int cols = 1;
                    do{
                        cur = cur->next;
                           if(col_entry[j].col_type == T_CHAR && cur->tok_value != STRING_LITERAL && cur->tok_value != K_NULL)
                              {
                                rc = DATA_MISMATCH;
                                cur->tok_value = INVALID;
                               break;
                               }
                           
                        else if(col_entry[j].col_type == T_INT && cur->tok_value != INT_LITERAL && cur->tok_value != K_NULL )
                            {
                                rc = DATA_MISMATCH;
                                cur->tok_value = INVALID;
                                break;
                            }
                        else{
                            int copy_size = 0;
                            if ( col_entry[j].col_type == T_CHAR )
                            {
                                if(col_entry[j].not_null!=1 && cur->tok_value == K_NULL)
                                {
                                    char str[1] = {0};
                                    memcpy((void*)((char*)new1_header+sizeof(table_file_header)+(new1_header->record_size * new1_header->num_records) + x),&copy_size,1);
                                    x=x+1;
                                    memcpy((void*)((char*)new1_header+sizeof(table_file_header)+(new1_header->record_size * new1_header->num_records) + x),str,0);
                                }
                                else if(col_entry[j].not_null==1 && cur->tok_value == K_NULL)
                                {
                                    rc = NULL_VALUE_INSERTED;
                                    cur->tok_value = INVALID;
                                    break;
                                }
                                else{
                                copy_size = strlen(cur->tok_string);
                                if(copy_size > col_entry[j].col_len )
                                {
                                    rc = INVALID_COLUMN_ENTRY;
                                    cur->tok_value = INVALID;
                                    break;
                                }
                                else{
                               
                                memcpy((void*)((char*)new1_header+sizeof(table_file_header)+(new1_header->record_size * new1_header->num_records) + x),&copy_size,1);
                                x=x+1;
                                memcpy((void*)((char*)new1_header+sizeof(table_file_header)+(new1_header->record_size * new1_header->num_records) + x),cur->tok_string,copy_size);
                                }
                                }
                            }
                            else if (col_entry[j].col_type == T_INT)
                            {
                                int value;
                                copy_size = 4;
                                
                                if(col_entry[j].not_null!=1 && cur->tok_value == K_NULL)
                                {
                                    value = atoi(cur->tok_string);
                                    int null = 0;
                                    memcpy((void*)((char*)new1_header+sizeof(table_file_header)+(new1_header->record_size * new1_header->num_records) + x),&null,1);
                                    x=x+1;
                                    memcpy((void*)((char*)new1_header+sizeof(table_file_header)+(new1_header->record_size * new1_header->num_records) + x),&value,copy_size);
                                }
                                else if(col_entry[j].not_null==1 && cur->tok_value == K_NULL)
                                {
                                    rc = NULL_VALUE_INSERTED;
                                    cur->tok_value = INVALID;
                                    break;
                                }
                                else{
                                if(atoi(cur->tok_string)<0)
                                {
                                    rc = INVALID_COLUMN_ENTRY;
                                    cur->tok_value = INVALID;
                                    break;
                                }
                                else{
                                    value = atoi(cur->tok_string);
                                    memcpy((void*)((char*)new1_header+sizeof(table_file_header)+(new1_header->record_size * new1_header->num_records) + x),&copy_size,1);
                                    x=x+1;
                                    memcpy((void*)((char*)new1_header+sizeof(table_file_header)+(new1_header->record_size * new1_header->num_records) + x),&value,copy_size);
                                   }
                                }
                            }
                                if(fhandle == NULL)
                                {
                                    rc = FILE_OPEN_ERROR;
                                    break;
                                }
                                else
                                {
                                    x = x+col_entry[j].col_len;
                                    j++;
                                }
                            }
                        
                        if(cur->next->tok_value == S_COMMA || cur->next->tok_value== S_RIGHT_PAREN)
                        {
                            if(temp_entry->num_columns >= cols)
                            {
                                if(cur->next->tok_value== S_RIGHT_PAREN && temp_entry->num_columns != cols)
                                {
                                    rc = INVALID_COLUMN_ENTRY;
                                    cur->tok_value = INVALID;
                                    break;
                                 }
                                else
                                { cur = cur->next;
                                       cols++;
                                }
                            }
                            else
                            {
                                rc = INVALID_COLUMN_ENTRY;
                                cur->tok_value = INVALID;
                                break;
                            }
                        }
                        else
                        {
                            rc = INVALID_SYNTAX;
                            cur->next->tok_value = INVALID;
                            break;
                        }
                    }while(cur->tok_value!=S_RIGHT_PAREN);
                    
                    if(rc!=0){
                        fwrite(new1_header, ((sizeof(table_file_header)+new1_header->record_size * new1_header->num_records)), 1, fhandle);
                        fflush(fhandle);
                        fclose(fhandle);
                        return rc;
                    }
                    if (cur->next->tok_value != EOC)
                    {
                        rc = INVALID_STATEMENT;
                        cur->next->tok_value = INVALID;
                    }
                    new1_header->num_records = new1_header->num_records + 1;
                    new1_header->file_size=sizeof(table_file_header)+(new1_header->record_size * new1_header->num_records);
                    fwrite(new1_header, ((sizeof(table_file_header)+new1_header->record_size * new1_header->num_records)), 1, fhandle);
                    fflush(fhandle);
                    fclose(fhandle);
                    printf("The size of %s is : %d\n",file_name,new1_header->file_size);
                    
                }
                }
        }
    
    return rc;
}
int sem_drop_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;

	cur = t_list;
	if ((cur->tok_class != keyword) &&
		  (cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if (cur->next->tok_value != EOC)
		{
			rc = INVALID_STATEMENT;
			cur->next->tok_value = INVALID;
		}
		else
		{
			if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
			{
				rc = TABLE_NOT_EXIST;
				cur->tok_value = INVALID;
			}
			else
			{
				/* Found a valid tpd, drop it from tpd list */
				rc = drop_tpd_from_list(cur->tok_string);
			}
		}
	}
    char table_name_remove[MAX_IDENT_LEN+4];
    char table1[MAX_IDENT_LEN+4];
    strcpy(table1,cur->tok_string);
    strcpy(table_name_remove,strcat(table1,".tab"));
    int del = remove(table_name_remove);
  return rc;
}

int sem_list_tables()
{
	int rc = 0;
	int num_tables = g_tpd_list->num_tables;
	tpd_entry *cur = &(g_tpd_list->tpd_start);

	if (num_tables == 0)
	{
		printf("\nThere are currently no tables defined\n");
	}
	else
	{
		printf("\nTable List\n");
		printf("*****************\n");
		while (num_tables-- > 0)
		{
			printf("%s\n", cur->table_name);
			if (num_tables > 0)
			{
				cur = (tpd_entry*)((char*)cur + cur->tpd_size);
			}
		}
		printf("****** End ******\n");
	}

  return rc;
}

int sem_list_schema(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	cd_entry  *col_entry = NULL;
	char tab_name[MAX_IDENT_LEN+1];
	char filename[MAX_IDENT_LEN+1];
	bool report = false;
	FILE *fhandle = NULL;
	int i = 0;

	cur = t_list;

	if (cur->tok_value != K_FOR)
  {
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
	}
	else
	{
		cur = cur->next;

		if ((cur->tok_class != keyword) &&
			  (cur->tok_class != identifier) &&
				(cur->tok_class != type_name))
		{
			// Error
			rc = INVALID_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			memset(filename, '\0', MAX_IDENT_LEN+1);
			strcpy(tab_name, cur->tok_string);
			cur = cur->next;

			if (cur->tok_value != EOC)
			{
				if (cur->tok_value == K_TO)
				{
					cur = cur->next;
					
					if ((cur->tok_class != keyword) &&
						  (cur->tok_class != identifier) &&
							(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_REPORT_FILE_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						if (cur->next->tok_value != EOC)
						{
							rc = INVALID_STATEMENT;
							cur->next->tok_value = INVALID;
						}
						else
						{
							/* We have a valid file name */
							strcpy(filename, cur->tok_string);
							report = true;
						}
					}
				}
				else
				{ 
					/* Missing the TO keyword */
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
				}
			}

			if (!rc)
			{
				if ((tab_entry = get_tpd_from_list(tab_name)) == NULL)
				{
					rc = TABLE_NOT_EXIST;
					cur->tok_value = INVALID;
				}
				else
				{
					if (report)
					{
						if((fhandle = fopen(filename, "a+tc")) == NULL)
						{
							rc = FILE_OPEN_ERROR;
						}
					}

					if (!rc)
					{
						/* Find correct tpd, need to parse column and index information */

						/* First, write the tpd_entry information */
						printf("Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
						printf("Table Name               (table_name)  = %s\n", tab_entry->table_name);
						printf("Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
						printf("Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
            printf("Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 

						if (report)
						{
							fprintf(fhandle, "Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
							fprintf(fhandle, "Table Name               (table_name)  = %s\n", tab_entry->table_name);
							fprintf(fhandle, "Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
							fprintf(fhandle, "Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
              fprintf(fhandle, "Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 
						}

						/* Next, write the cd_entry information */
						for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
								i < tab_entry->num_columns; i++, col_entry++)
						{
							printf("Column Name   (col_name) = %s\n", col_entry->col_name);
							printf("Column Id     (col_id)   = %d\n", col_entry->col_id);
							printf("Column Type   (col_type) = %d\n", col_entry->col_type);
							printf("Column Length (col_len)  = %d\n", col_entry->col_len);
							printf("Not Null flag (not_null) = %d\n\n", col_entry->not_null);

							if (report)
							{
								fprintf(fhandle, "Column Name   (col_name) = %s\n", col_entry->col_name);
								fprintf(fhandle, "Column Id     (col_id)   = %d\n", col_entry->col_id);
								fprintf(fhandle, "Column Type   (col_type) = %d\n", col_entry->col_type);
								fprintf(fhandle, "Column Length (col_len)  = %d\n", col_entry->col_len);
								fprintf(fhandle, "Not Null Flag (not_null) = %d\n\n", col_entry->not_null);
							}
						}
	
						if (report)
						{
							fflush(fhandle);
							fclose(fhandle);
						}
					} // File open error							
				} // Table not exist
			} // no semantic errors
		} // Invalid table name
	} // Invalid statement

  return rc;
}

int initialize_tpd_list()
{
	int rc = 0;
	FILE *fhandle = NULL;
//	struct _stat file_stat;
	struct stat file_stat;

  /* Open for read */
  if((fhandle = fopen("dbfile.bin", "rbc")) == NULL)
	{
		if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
		{
			rc = FILE_OPEN_ERROR;
		}
    else
		{
			g_tpd_list = NULL;
			g_tpd_list = (tpd_list*)calloc(1, sizeof(tpd_list));
			
			if (!g_tpd_list)
			{
				rc = MEMORY_ERROR;
			}
			else
			{
				g_tpd_list->list_size = sizeof(tpd_list);
				fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
				fflush(fhandle);
				fclose(fhandle);
			}
		}
	}
	else
	{
		/* There is a valid dbfile.bin file - get file size */
//		_fstat(_fileno(fhandle), &file_stat);
		fstat(fileno(fhandle), &file_stat);
		printf("dbfile.bin size = %d\n", file_stat.st_size);

		g_tpd_list = (tpd_list*)calloc(1, file_stat.st_size);

		if (!g_tpd_list)
		{
			rc = MEMORY_ERROR;
		}
		else
		{
			fread(g_tpd_list, file_stat.st_size, 1, fhandle);
			fflush(fhandle);
			fclose(fhandle);

			if (g_tpd_list->list_size != file_stat.st_size)
			{
				rc = DBFILE_CORRUPTION;
			}

		}
	}
    
	return rc;
}
	
int add_tpd_to_list(tpd_entry *tpd)
{
	int rc = 0;
	int old_size = 0;
	FILE *fhandle = NULL;

	if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
  else
	{
		old_size = g_tpd_list->list_size;

		if (g_tpd_list->num_tables == 0)
		{
			/* If this is an empty list, overlap the dummy header */
			g_tpd_list->num_tables++;
		 	g_tpd_list->list_size += (tpd->tpd_size - sizeof(tpd_entry));
			fwrite(g_tpd_list, old_size - sizeof(tpd_entry), 1, fhandle);
		}
		else
		{
			/* There is at least 1, just append at the end */
			g_tpd_list->num_tables++;
		 	g_tpd_list->list_size += tpd->tpd_size;
			fwrite(g_tpd_list, old_size, 1, fhandle);
		}

		fwrite(tpd, tpd->tpd_size, 1, fhandle);
		fflush(fhandle);
		fclose(fhandle);
	}

	return rc;
}

int drop_tpd_from_list(char *tabname)
{
	int rc = 0;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;
	int count = 0;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (strcasecmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				int old_size = 0;
				FILE *fhandle = NULL;

				if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
				{
					rc = FILE_OPEN_ERROR;
				}
			  else
				{
					old_size = g_tpd_list->list_size;

					if (count == 0)
					{
						/* If this is the first entry */
						g_tpd_list->num_tables--;

						if (g_tpd_list->num_tables == 0)
						{
							/* This is the last table, null out dummy header */
							memset((void*)g_tpd_list, '\0', sizeof(tpd_list));
							g_tpd_list->list_size = sizeof(tpd_list);
							fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
						}
						else
						{
							/* First in list, but not the last one */
							g_tpd_list->list_size -= cur->tpd_size;

							/* First, write the 8 byte header */
							fwrite(g_tpd_list, sizeof(tpd_list) - sizeof(tpd_entry),
								     1, fhandle);

							/* Now write everything starting after the cur entry */
							fwrite((char*)cur + cur->tpd_size,
								     old_size - cur->tpd_size -
										 (sizeof(tpd_list) - sizeof(tpd_entry)),
								     1, fhandle);
						}
					}
					else
					{
						/* This is NOT the first entry - count > 0 */
						g_tpd_list->num_tables--;
					 	g_tpd_list->list_size -= cur->tpd_size;

						/* First, write everything from beginning to cur */
						fwrite(g_tpd_list, ((char*)cur - (char*)g_tpd_list),
									 1, fhandle);

						/* Check if cur is the last entry. Note that g_tdp_list->list_size
						   has already subtracted the cur->tpd_size, therefore it will
						   point to the start of cur if cur was the last entry */
						if ((char*)g_tpd_list + g_tpd_list->list_size == (char*)cur)
						{
							/* If true, nothing else to write */
						}
						else
						{
							/* NOT the last entry, copy everything from the beginning of the
							   next entry which is (cur + cur->tpd_size) and the remaining size */
							fwrite((char*)cur + cur->tpd_size,
										 old_size - cur->tpd_size -
										 ((char*)cur - (char*)g_tpd_list),							     
								     1, fhandle);
						}
					}

					fflush(fhandle);
					fclose(fhandle);
				}

				
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
					count++;
				}
			}
		}
	}
	
	if (!found)
	{
		rc = INVALID_TABLE_NAME;
	}
    
	return rc;
}

tpd_entry* get_tpd_from_list(char *tabname)
{
	tpd_entry *tpd = NULL;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (strcasecmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				tpd = cur;
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
				}
			}
		}
	}

	return tpd;
}

