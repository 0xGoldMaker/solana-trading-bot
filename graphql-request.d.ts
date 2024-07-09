declare module 'graphql-request' {
  export const gql: any;
  export class GraphQLClient {
    constructor(endpoint: string, options?: any);
    request(query: string, variables?: any): Promise<any>;
  }
}