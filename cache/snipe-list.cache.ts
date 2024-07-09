import fs from 'fs';
import path from 'path';
import { logger, SNIPE_LIST_REFRESH_INTERVAL } from '../helpers';
import { EventEmitter } from 'events';

export class SnipeListCache extends EventEmitter {
  private snipeList: string[] = [];
  private fileLocation = path.join(__dirname, '../snipe-list.txt');

  constructor() {
    super();
    setInterval(() => this.loadSnipeList(), SNIPE_LIST_REFRESH_INTERVAL);
  }

  public init() {
    this.loadSnipeList();
  }

  public isInList(mint: string): boolean {
    return this.snipeList.includes(mint);
  }

  public getList(): string[] {
    return [...this.snipeList]; // Return a copy of the list to prevent external modifications
  }

  private loadSnipeList() {
    logger.trace(`Refreshing snipe list...`);

    const oldList = [...this.snipeList];
    const data = fs.readFileSync(this.fileLocation, 'utf-8');
    this.snipeList = data
      .split('\n')
      .map((a) => a.trim())
      .filter((a) => a);

    if (this.snipeList.length != oldList.length) {
      logger.info(`Loaded snipe list: ${this.snipeList.length}`);
      
      // Emit events for new tokens
      this.snipeList.forEach(token => {
        if (!oldList.includes(token)) {
          logger.info(`New token added to snipe list: ${token}`);
          this.emit('newToken', token);
        }
      });
    }
  }
}